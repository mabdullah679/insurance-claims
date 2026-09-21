"""Publish successful claims to Kafka -- requirements section 3, and the
retry/failure rules from section 4.

The load-bearing requirement is ACKNOWLEDGEMENT ACCOUNTING:

    "Require producer acknowledgments from Kafka before the DAG treats a
     message as published."
    "Acknowledged Kafka count must equal the successful claim count."

So this module counts what the BROKER confirmed, never what it handed to the
client library. `producer.send()` returning without raising means the message
is in a buffer; it says nothing about durability. Every send's future is
resolved and only a resolved-without-error future increments `acknowledged`.

Delivery semantics, per the requirements:
  * acks=all + idempotent producer -> at-least-once without duplicates
  * claim_id as the message key    -> per-claim ordering within a partition
  * bounded batches                -> broker and message-size limits
  * TLS + SASL                     -> from environment, never hardcoded

RETRY AND FAILURE. The client library retries within a send; this module adds
an outer retry over the batch for messages that came back unacknowledged, up
to `max_retries` (default 3, per section 4), with a configured delay between
attempts. After exhaustion: unpublishable events go to the dead-letter topic
IF one is configured, and then a PublishError is raised. The DAG must fail --
and, critically, must not archive the input file.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterator

from claims.common.config import KafkaConfig
from claims.common.outcomes import PublishError

log = logging.getLogger(__name__)

# The message contract, in the order the requirements table lists it.
MESSAGE_FIELDS = ("claim_id", "customer_id", "policy_id", "claim_date",
                  "claim_amount", "risk_score", "processing_status",
                  "processing_timestamp", "dag_run_id", "schema_version")

PROCESSING_STATUS_SUCCESS = "SUCCESS"


def _json_default(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        # str, not float: these are monetary values and a consumer that wants
        # exactness must not be handed binary floating point.
        return str(v)
    return str(v)


def build_message(claim: dict[str, Any], *, dag_run_id: str, schema_version: str,
                  processed_at: datetime | None = None) -> dict[str, Any]:
    """One claim as the Kafka event the requirements specify.

    Exactly the ten contracted fields, and nothing else: enrichment columns
    like customer_name stay out of the topic. Downstream consumers were
    promised a schema, and quietly widening it is how a schema stops meaning
    anything.
    """
    ts = processed_at or datetime.now(timezone.utc)
    return {
        "claim_id": claim["claim_id"],
        "customer_id": claim["customer_id"],
        "policy_id": claim["policy_id"],
        "claim_date": _iso(claim.get("claim_date")),
        "claim_amount": _amount(claim.get("claim_amount")),
        "risk_score": claim.get("risk_score"),
        "processing_status": PROCESSING_STATUS_SUCCESS,
        "processing_timestamp": ts.isoformat(),
        "dag_run_id": dag_run_id,
        "schema_version": schema_version,
    }


def validate_message(msg: dict[str, Any]) -> None:
    """Refuse to publish an event that violates the contract.

    Requirement: "Every event must satisfy the registered message schema."
    Without a schema registry wired up this is the enforcement point, and it
    runs even when one is -- a registry rejects on the broker side after the
    network round trip, which is a worse place to find out.
    """
    missing = [f for f in MESSAGE_FIELDS if f not in msg]
    if missing:
        raise PublishError(f"message missing required field(s): {missing}")
    if not msg["claim_id"]:
        raise PublishError("message has an empty claim_id, which is the Kafka key")
    score = msg.get("risk_score")
    if not isinstance(score, int):
        raise PublishError(
            f"claim {msg['claim_id']}: risk_score must be an int, got {score!r}")


class ClaimsProducer:
    """Thin wrapper over KafkaProducer that resolves every send's future.

    The producer is injectable so the whole publish path is testable with no
    broker; `tests/test_kafka_publish.py` drives failure and partial-ack cases
    through a fake that would be impossible to provoke reliably against a real
    one.
    """

    def __init__(self, cfg: KafkaConfig, producer: Any = None):
        self.cfg = cfg
        self._producer = producer
        self._owned = producer is None

    def producer_kwargs(self) -> dict[str, Any]:
        """Exactly what the real KafkaProducer is constructed with.

        Separated from `__enter__` so the configuration can be asserted
        without opening a socket. `tests/test_kafka_publish.py` checks both
        that these settings carry the delivery semantics the requirements
        demand AND that the installed client actually accepts every one of
        them -- the kafka-python-ng fork did not, which meant the publish task
        died at construction while every test passed against a fake producer.
        """
        return {
            "bootstrap_servers": self.cfg.bootstrap.split(","),
            "value_serializer": lambda v: json.dumps(v, default=_json_default).encode(),
            "key_serializer": lambda k: None if k is None else str(k).encode(),
            # An unacknowledged message must never count as published.
            "acks": "all",
            # Idempotence makes the library's own retries safe: at-least-once
            # delivery without producing duplicate records on the partition.
            "enable_idempotence": True,
            "max_in_flight_requests_per_connection": 5,
            "retries": self.cfg.max_retries,
            "request_timeout_ms": self.cfg.request_timeout_ms,
            "linger_ms": 50,
            **_security_config(self.cfg.conn_id),
        }

    def __enter__(self) -> "ClaimsProducer":
        if self._producer is None:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(**self.producer_kwargs())
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._owned and self._producer is not None:
            try:
                self._producer.flush(timeout=30)
            finally:
                self._producer.close(timeout=30)

    def send_batch(self, topic: str, messages: list[dict[str, Any]]) -> tuple[int, list[tuple[dict, str]]]:
        """Send a batch and resolve every future. Returns (acked, failures).

        Futures are collected first and resolved second so the batch pipelines
        across the network instead of serialising on each ack.
        """
        futures = []
        failures: list[tuple[dict, str]] = []
        for msg in messages:
            try:
                fut = self._producer.send(topic, key=msg["claim_id"], value=msg)
                futures.append((msg, fut))
            except Exception as e:                      # buffer full, serialization
                failures.append((msg, f"send failed: {e}"))

        self._producer.flush(timeout=self.cfg.request_timeout_ms / 1000)

        acked = 0
        for msg, fut in futures:
            try:
                fut.get(timeout=self.cfg.request_timeout_ms / 1000)
                acked += 1
            except Exception as e:                      # broker refused / timed out
                failures.append((msg, f"not acknowledged: {e}"))
        return acked, failures


def _batches(items: list[Any], size: int) -> Iterator[list[Any]]:
    for i in range(0, len(items), max(1, size)):
        yield items[i:i + size]


def run(claims: list[dict[str, Any]], *, dag_run_id: str,
        cfg: KafkaConfig | None = None, producer: Any = None,
        processed_at: datetime | None = None,
        sleep=time.sleep) -> dict[str, Any]:
    """Publish every successful claim, or fail the DAG trying.

    Returns the metrics section 5 asks for: attempted, acknowledged, failed,
    retried, and dead-lettered counts.

    `processed_at` is injectable so a corrected re-run for the same logical
    date can carry a deterministic timestamp, and so tests are not racing a
    clock.
    """
    cfg = cfg or KafkaConfig()
    if not claims:
        log.info("no successful claims to publish")
        return {"attempted": 0, "acknowledged": 0, "failed": 0, "retries": 0,
                "dead_lettered": 0, "topic": cfg.topic,
                "schema_version": cfg.schema_version}

    messages = [build_message(c, dag_run_id=dag_run_id,
                              schema_version=cfg.schema_version,
                              processed_at=processed_at)
                for c in claims]
    for m in messages:
        validate_message(m)

    attempted = len(messages)
    acknowledged = 0
    retries_used = 0
    pending = messages
    last_failures: list[tuple[dict, str]] = []

    with ClaimsProducer(cfg, producer=producer) as p:
        # Attempt 0 is the initial try; 1..max_retries are the retries the
        # requirements allow ("up to three times after the initial attempt").
        for attempt in range(cfg.max_retries + 1):
            if not pending:
                break
            if attempt:
                retries_used += 1
                log.warning("kafka publish retry %d/%d for %d message(s)",
                            attempt, cfg.max_retries, len(pending))
                sleep(cfg.retry_delay_seconds)

            failures: list[tuple[dict, str]] = []
            for batch in _batches(pending, cfg.batch_size):
                acked, batch_failures = p.send_batch(cfg.topic, batch)
                acknowledged += acked
                failures.extend(batch_failures)

            last_failures = failures
            pending = [m for m, _ in failures]

        # Retries are exhausted. Anything still pending is unpublishable.
        if pending:
            dead_lettered = _dead_letter(p, cfg, last_failures)
            error = PublishError(
                f"{len(pending)} of {attempted} claim(s) were not acknowledged "
                f"by Kafka after {cfg.max_retries} retries "
                f"(topic {cfg.topic}, dead-lettered {dead_lettered}). "
                f"First failure: {last_failures[0][1] if last_failures else 'unknown'}. "
                f"The input file must NOT be archived; re-run this logical date "
                f"once the broker is healthy.")
            # Section 5 requires failure, retry and dead-letter counts as
            # metrics. Raising without them would leave the only record of
            # what happened inside this message, so the caller can persist
            # them from the exception.
            error.metrics = {
                "attempted": attempted, "acknowledged": acknowledged,
                "failed": len(pending), "retries": retries_used,
                "dead_lettered": dead_lettered, "topic": cfg.topic,
                "schema_version": cfg.schema_version,
            }
            raise error

    if acknowledged != attempted:
        # Defensive: the loop above should make this unreachable. It is checked
        # anyway because the count is what the reconciliation trusts, and a
        # silent over-count would let a short run archive its input.
        raise PublishError(
            f"acknowledged {acknowledged} != attempted {attempted}; refusing "
            f"to report a successful publish")

    log.info("published %d claim(s) to %s (%d retries)",
             acknowledged, cfg.topic, retries_used)
    return {"attempted": attempted, "acknowledged": acknowledged, "failed": 0,
            "retries": retries_used, "dead_lettered": 0, "topic": cfg.topic,
            "schema_version": cfg.schema_version}


def _dead_letter(p: ClaimsProducer, cfg: KafkaConfig,
                 failures: list[tuple[dict, str]]) -> int:
    """Best-effort DLQ write. Requirement: "when configured".

    Never raises: the DAG is already failing on the caller's side, and losing
    the real error behind a DLQ error would make the failure harder to
    diagnose, not easier.
    """
    if not cfg.dlq_topic or not failures:
        return 0
    envelopes = [{**msg, "failure_reason": reason,
                  "failed_at": datetime.now(timezone.utc).isoformat()}
                 for msg, reason in failures]
    try:
        acked, _ = p.send_batch(cfg.dlq_topic, envelopes)
        log.error("dead-lettered %d unpublishable event(s) to %s", acked, cfg.dlq_topic)
        return acked
    except Exception as e:
        log.error("dead-letter write to %s failed: %s", cfg.dlq_topic, e)
        return 0


def _iso(v: Any) -> Any:
    return v.isoformat() if isinstance(v, (date, datetime)) else v


def _amount(v: Any) -> Any:
    return str(v) if isinstance(v, Decimal) else v


def _security_config(conn_id: str | None = None) -> dict[str, Any]:
    """TLS and SASL for the producer. Requirement: TLS in transit.

    Requirements section 3 asks for credentials in "an approved secret manager
    or Airflow connection", so an Airflow connection is the primary source and
    is consulted first. Recognised fields:

        login / password        -> SASL username and password
        extra.security_protocol -> e.g. SASL_SSL, SSL
        extra.sasl_mechanism    -> defaults to PLAIN when a login is present
        extra.ssl_cafile / ssl_certfile / ssl_keyfile

    Environment variables remain as a fallback for local development only, and
    the connection wins wherever both are set. Returns {} when neither is
    configured, so a local plaintext broker stays usable in tests without a
    second code path. No credential is ever logged.
    """
    cfg: dict[str, Any] = {}

    # Environment first, so the connection can override it below.
    if protocol := os.environ.get("KAFKA_SECURITY_PROTOCOL"):
        cfg["security_protocol"] = protocol
    if cafile := os.environ.get("KAFKA_SSL_CAFILE"):
        cfg["ssl_cafile"] = cafile
    if certfile := os.environ.get("KAFKA_SSL_CERTFILE"):
        cfg["ssl_certfile"] = certfile
    if keyfile := os.environ.get("KAFKA_SSL_KEYFILE"):
        cfg["ssl_keyfile"] = keyfile
    if user := os.environ.get("KAFKA_SASL_USERNAME"):
        cfg["sasl_mechanism"] = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")
        cfg["sasl_plain_username"] = user
        cfg["sasl_plain_password"] = os.environ.get("KAFKA_SASL_PASSWORD", "")
        cfg.setdefault("security_protocol", "SASL_SSL")

    if not conn_id:
        return cfg

    try:
        from airflow.sdk import Connection
        conn = Connection.get(conn_id)
    except Exception as e:
        # Publishing without the credential would fail at the broker anyway,
        # and failing here names the real cause.
        raise PublishError(
            f"could not resolve Kafka connection {conn_id!r}: {e}") from e

    if conn is None:
        raise PublishError(f"Kafka connection {conn_id!r} is not defined")

    try:
        extra = conn.extra_dejson or {}
    except Exception:
        log.warning("Kafka connection %s has unparseable extra", conn_id)
        extra = {}

    for key in ("security_protocol", "sasl_mechanism", "ssl_cafile",
                "ssl_certfile", "ssl_keyfile", "ssl_password"):
        if value := extra.get(key):
            cfg[key] = value

    if conn.login:
        cfg["sasl_plain_username"] = conn.login
        cfg["sasl_plain_password"] = conn.password or ""
        cfg.setdefault("sasl_mechanism", "PLAIN")
        cfg.setdefault("security_protocol", "SASL_SSL")

    log.info("kafka security configured from connection %s (protocol=%s)",
             conn_id, cfg.get("security_protocol", "PLAINTEXT"))
    return cfg
