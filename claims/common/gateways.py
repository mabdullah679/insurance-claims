"""Adapters to S3, PostgreSQL and the policy API.

Each one has ONE job beyond the I/O: translate a transport failure into
`DependencyError` / `InputFileError`, and an empty-but-successful answer into
an empty result. The stages downstream depend on that distinction to decide
between rejecting a record and failing the DAG (see claims/enrich/join.py).

Airflow hooks are imported INSIDE methods, not at module import, so the
`claims` package stays importable -- and unit-testable -- without Airflow
installed. The tests exercise every stage through fakes that honour these same
contracts.
"""
from __future__ import annotations

import csv
import io
import logging
from typing import Any

from claims.common.config import EnrichmentConfig, S3Config
from claims.common.outcomes import DependencyError, InputFileError

log = logging.getLogger(__name__)


class S3Gateway:
    """Read the inbound file, write rejects, archive the original."""

    def __init__(self, cfg: S3Config, hook: Any = None):
        self.cfg = cfg
        self._hook = hook

    @property
    def hook(self) -> Any:
        if self._hook is None:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            self._hook = S3Hook(aws_conn_id=self.cfg.conn_id)
        return self._hook

    def exists(self, key: str) -> bool:
        try:
            return bool(self.hook.check_for_key(key, bucket_name=self.cfg.bucket))
        except Exception as e:
            raise DependencyError(
                f"could not check s3://{self.cfg.bucket}/{key}: {e}") from e

    def read(self, key: str) -> bytes:
        """Fetch the object. Missing or empty is an InputFileError.

        Emptiness is checked here rather than left to the CSV parser so the
        error names the S3 key -- the operator's next action is to look at
        that object, and the message should tell them which one.
        """
        if not self.exists(key):
            raise InputFileError(
                f"expected claims file s3://{self.cfg.bucket}/{key} does not exist")
        try:
            obj = self.hook.get_key(key, bucket_name=self.cfg.bucket)
            body = obj.get()["Body"].read()
        except Exception as e:
            raise DependencyError(
                f"could not read s3://{self.cfg.bucket}/{key}: {e}") from e
        if not body or not body.strip():
            raise InputFileError(
                f"claims file s3://{self.cfg.bucket}/{key} is empty")
        return body

    def write_rejections(self, key: str, rows: list[dict[str, Any]]) -> int:
        """Write rejected records with their reasons. Returns the row count.

        The count is returned rather than assumed so reconciliation can verify
        that the promise "every rejected claim is stored with a reason" was
        actually kept, rather than trusting that this method ran.
        """
        if not rows:
            return 0
        # Union of keys, stable order: rejected rows come from different
        # stages and do not all carry the same columns.
        header: list[str] = []
        for row in rows:
            for k in row:
                if k not in header:
                    header.append(k)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _csv_safe(row.get(k)) for k in header})

        try:
            self.hook.load_string(buf.getvalue(), key=key,
                                  bucket_name=self.cfg.bucket, replace=True)
        except Exception as e:
            raise DependencyError(
                f"could not write rejections to s3://{self.cfg.bucket}/{key}: {e}") from e
        log.info("wrote %d rejected claim(s) to s3://%s/%s",
                 len(rows), self.cfg.bucket, key)
        return len(rows)

    def archive(self, source_key: str, dest_key: str) -> None:
        """Move the input file. Called ONLY after required work succeeded.

        Copy-then-delete rather than a move: if the delete fails the file is
        in both places, which is recoverable. The reverse is not.
        """
        try:
            self.hook.copy_object(source_bucket_key=source_key,
                                  dest_bucket_key=dest_key,
                                  source_bucket_name=self.cfg.bucket,
                                  dest_bucket_name=self.cfg.bucket)
            self.hook.delete_objects(bucket=self.cfg.bucket, keys=[source_key])
        except Exception as e:
            raise DependencyError(
                f"could not archive s3://{self.cfg.bucket}/{source_key} "
                f"to {dest_key}: {e}") from e
        log.info("archived s3://%s/%s -> %s", self.cfg.bucket, source_key, dest_key)


class PostgresCustomerLookup:
    """Batch customer fetch. Satisfies claims.enrich.join.CustomerLookup."""

    def __init__(self, cfg: EnrichmentConfig, hook: Any = None):
        self.cfg = cfg
        self._hook = hook

    @property
    def hook(self) -> Any:
        if self._hook is None:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            self._hook = PostgresHook(postgres_conn_id=self.cfg.postgres_conn_id)
        return self._hook

    def fetch(self, customer_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not customer_ids:
            return {}
        # Parameterised IN list: the ids come from a customer-supplied file and
        # must never be interpolated into SQL. The table name is configuration,
        # not user input, and is validated as an identifier before use.
        _assert_identifier(self.cfg.customer_table)
        placeholders = ", ".join(["%s"] * len(customer_ids))
        sql = (f"SELECT customer_id, customer_name, date_of_birth, state, "
               f"customer_since, customer_status "
               f"FROM {self.cfg.customer_table} "
               f"WHERE customer_id IN ({placeholders})")
        try:
            rows = self.hook.get_records(sql, parameters=list(customer_ids))
        except Exception as e:
            # The database did not answer. Every claim in this run is now
            # unknowable, not invalid.
            raise DependencyError(
                f"customer lookup against {self.cfg.postgres_conn_id} failed: {e}") from e

        cols = ("customer_id", "customer_name", "date_of_birth", "state",
                "customer_since", "customer_status")
        found = {r[0]: dict(zip(cols, r)) for r in rows or []}
        log.info("customer lookup: %d requested, %d found",
                 len(customer_ids), len(found))
        return found


class PolicyApiLookup:
    """Batch policy fetch over REST. Satisfies claims.enrich.join.PolicyLookup.

    Retries transient failures `policy_api_attempts` times before giving up,
    because a single flaky response should not fail a daily run -- but an API
    that is genuinely down must still raise rather than return an empty dict,
    which would reject every claim.
    """

    def __init__(self, cfg: EnrichmentConfig, session: Any = None, sleep=None):
        self.cfg = cfg
        self._session = session
        import time
        self._sleep = sleep or time.sleep

    @property
    def session(self) -> Any:
        """A session carrying the credential from the Airflow connection.

        Requirements section 3: "store task-relevant credentials in an
        approved secret manager or Airflow connection." The connection named
        by `policy_api_conn_id` supplies the credential; nothing is read from
        the environment and no secret is logged.

        Supported shapes, in precedence order:
          * `password` with no `login`  -> Bearer token
          * `login` + `password`        -> HTTP basic auth
          * `extra.headers`             -> merged in (e.g. an API-key header)
        `host` on the connection overrides `CLAIMS_POLICY_API_BASE`, so an
        environment can be repointed without a code change.
        """
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._apply_connection(self._session)
        return self._session

    def _apply_connection(self, session: Any) -> None:
        conn_id = self.cfg.policy_api_conn_id
        if not conn_id:
            log.warning("no policy API connection configured; requests will be "
                        "unauthenticated")
            return
        try:
            from airflow.sdk import Connection
            conn = Connection.get(conn_id)
        except Exception as e:
            # An unreachable metadata DB or a missing connection is a
            # dependency problem, not a data problem: every claim in this run
            # would otherwise be rejected for lack of a policy.
            raise DependencyError(
                f"could not resolve policy API connection {conn_id!r}: {e}") from e

        if conn is None:
            raise DependencyError(
                f"policy API connection {conn_id!r} is not defined")

        extra = {}
        try:
            extra = conn.extra_dejson or {}
        except Exception:                       # malformed extra JSON
            log.warning("policy API connection %s has unparseable extra", conn_id)

        headers = extra.get("headers")
        if isinstance(headers, dict):
            session.headers.update(headers)

        if conn.password and not conn.login:
            session.headers["Authorization"] = f"Bearer {conn.password}"
        elif conn.login and conn.password:
            session.auth = (conn.login, conn.password)
        elif not headers:
            log.warning("policy API connection %s carries no credential; "
                        "requests will be unauthenticated", conn_id)

        if conn.host:
            self._base_override = conn.host

        log.info("policy API session configured from connection %s", conn_id)

    @property
    def base_url(self) -> str:
        """Connection host wins over the configured default."""
        return getattr(self, "_base_override", None) or self.cfg.policy_api_base

    def fetch(self, policy_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not policy_ids:
            return {}
        found: dict[str, dict[str, Any]] = {}
        size = max(1, self.cfg.policy_api_batch_size)
        for i in range(0, len(policy_ids), size):
            batch = policy_ids[i:i + size]
            found.update(self._fetch_batch(batch))
        log.info("policy lookup: %d requested, %d found", len(policy_ids), len(found))
        return found

    def _fetch_batch(self, batch: list[str]) -> dict[str, dict[str, Any]]:
        # `self.session` first: resolving the connection may override the base
        # URL, and reading `base_url` before that would use the stale default.
        session = self.session
        url = f"{self.base_url.rstrip('/')}/policies"
        last: Exception | None = None
        for attempt in range(1, self.cfg.policy_api_attempts + 1):
            try:
                resp = session.get(
                    url, params={"policy_ids": ",".join(batch)},
                    timeout=self.cfg.policy_api_timeout)
                resp.raise_for_status()
                payload = resp.json()
                items = payload.get("policies", payload) if isinstance(payload, dict) else payload
                return {p["policy_id"]: p for p in items if p.get("policy_id")}
            except Exception as e:
                last = e
                log.warning("policy API attempt %d/%d failed: %s",
                            attempt, self.cfg.policy_api_attempts, e)
                if attempt < self.cfg.policy_api_attempts:
                    self._sleep(min(2 ** attempt, 30))
        raise DependencyError(
            f"policy API {url} failed after {self.cfg.policy_api_attempts} "
            f"attempt(s): {last}")


def _assert_identifier(name: str) -> None:
    """Guard the one place a config value reaches SQL unparameterised."""
    parts = name.split(".")
    if not parts or not all(p.replace("_", "").isalnum() and p for p in parts):
        raise ValueError(f"unsafe SQL identifier in configuration: {name!r}")


def _csv_safe(v: Any) -> Any:
    from datetime import date, datetime
    from decimal import Decimal
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    return v
