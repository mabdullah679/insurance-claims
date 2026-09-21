"""Gateway adapters -- S3, PostgreSQL, and the policy API.

`claims/common/gateways.py` had no test file before this one. That mattered:
the policy API client was sending unauthenticated requests while
`policy_api_conn_id` sat in the configuration unread, which is the failure
mode of "a control that was never code".

Every hook and session here is injected, so nothing in this file opens a
socket or needs a running Airflow.
"""
from __future__ import annotations

import json

import pytest

from claims.common.config import EnrichmentConfig, S3Config
from claims.common.gateways import (PolicyApiLookup, PostgresCustomerLookup,
                                    S3Gateway, _assert_identifier)
from claims.common.outcomes import DependencyError, InputFileError


class FakeS3Hook:
    """Honours the S3Hook surface the gateway actually uses."""

    def __init__(self, objects=None, fail=False):
        self.objects = objects or {}
        self.fail = fail
        self.loaded = {}
        self.copied = []
        self.deleted = []

    def check_for_key(self, key, bucket_name=None):
        if self.fail:
            raise RuntimeError("S3 unreachable")
        return key in self.objects

    def get_key(self, key, bucket_name=None):
        body = self.objects[key]

        class Obj:
            def get(self_inner):
                return {"Body": type("B", (), {"read": lambda s: body})()}

        return Obj()

    def load_string(self, value, key=None, bucket_name=None, replace=False):
        self.loaded[key] = value

    def copy_object(self, source_bucket_key=None, dest_bucket_key=None,
                    source_bucket_name=None, dest_bucket_name=None):
        self.copied.append((source_bucket_key, dest_bucket_key))

    def delete_objects(self, bucket=None, keys=None):
        self.deleted.extend(keys or [])


class TestS3Read:
    """Requirements 1-2: the file must exist and must not be empty."""

    def test_missing_object_is_an_input_file_error(self):
        gw = S3Gateway(S3Config(), hook=FakeS3Hook())
        with pytest.raises(InputFileError, match="does not exist"):
            gw.read("inbound/claims_20260911.csv")

    def test_empty_object_is_an_input_file_error(self):
        """Requirement 2, caught here so the message names the S3 key."""
        hook = FakeS3Hook({"inbound/claims_20260911.csv": b"   \n"})
        gw = S3Gateway(S3Config(), hook=hook)
        with pytest.raises(InputFileError, match="is empty"):
            gw.read("inbound/claims_20260911.csv")

    def test_an_unreachable_bucket_is_a_dependency_error_not_a_rejection(self):
        """An outage must never be mistaken for bad data."""
        gw = S3Gateway(S3Config(), hook=FakeS3Hook(fail=True))
        with pytest.raises(DependencyError):
            gw.read("inbound/claims_20260911.csv")

    def test_a_good_object_is_returned_verbatim(self):
        raw = b"claim_id,customer_id\nC1,CUS1\n"
        gw = S3Gateway(S3Config(), hook=FakeS3Hook({"k": raw}))
        assert gw.read("k") == raw


class TestS3Rejections:
    """Requirement 12: rejects to the error folder, with reasons."""

    def test_rows_are_written_with_their_reason_columns(self):
        hook = FakeS3Hook()
        gw = S3Gateway(S3Config(), hook=hook)
        count = gw.write_rejections("errors/dt=20260911/r.csv", [
            {"claim_id": "C1", "rejection_reason": "DUPLICATE_CLAIM_ID",
             "rejection_detail": "row 3"},
        ])
        written = hook.loaded["errors/dt=20260911/r.csv"]
        assert count == 1
        assert "rejection_reason" in written
        assert "DUPLICATE_CLAIM_ID" in written

    def test_the_returned_count_is_what_reconciliation_checks(self):
        """"Every rejected claim must be stored" is verified, not assumed."""
        gw = S3Gateway(S3Config(), hook=FakeS3Hook())
        assert gw.write_rejections("k", [{"claim_id": f"C{i}"} for i in range(4)]) == 4

    def test_nothing_to_store_writes_nothing(self):
        hook = FakeS3Hook()
        assert S3Gateway(S3Config(), hook=hook).write_rejections("k", []) == 0
        assert hook.loaded == {}

    def test_rows_from_different_stages_keep_all_their_columns(self):
        """Validation and enrichment rejects do not share a schema."""
        hook = FakeS3Hook()
        S3Gateway(S3Config(), hook=hook).write_rejections("k", [
            {"claim_id": "C1", "rejection_reason": "A"},
            {"claim_id": "C2", "policy_id": "P2", "rejection_reason": "B"},
        ])
        header = hook.loaded["k"].splitlines()[0]
        assert "policy_id" in header


class TestS3Archive:
    """Requirement 14, and the safety property behind it."""

    def test_archive_copies_before_deleting(self):
        """If the delete fails the file is in both places, which is recoverable."""
        hook = FakeS3Hook({"inbound/c.csv": b"x"})
        S3Gateway(S3Config(), hook=hook).archive("inbound/c.csv", "archive/c.csv")
        assert hook.copied == [("inbound/c.csv", "archive/c.csv")]
        assert hook.deleted == ["inbound/c.csv"]


class TestCustomerLookup:
    """Requirement 6, and the SQL-injection guard on the table name."""

    class FakePgHook:
        def __init__(self, rows=None, fail=False):
            self.rows = rows or []
            self.fail = fail
            self.sql = None
            self.parameters = None

        def get_records(self, sql, parameters=None):
            if self.fail:
                raise RuntimeError("could not connect")
            self.sql, self.parameters = sql, parameters
            return self.rows

    def test_ids_are_passed_as_parameters_never_interpolated(self):
        """The ids come from a customer-supplied file."""
        hook = self.FakePgHook(rows=[])
        PostgresCustomerLookup(EnrichmentConfig(), hook=hook).fetch(
            ["CUS-1'; DROP TABLE customers;--"])
        assert "DROP TABLE" not in hook.sql
        assert hook.parameters == ["CUS-1'; DROP TABLE customers;--"]

    def test_a_database_outage_raises_rather_than_returning_empty(self):
        """Returning {} would reject every claim in the run as unknown."""
        lookup = PostgresCustomerLookup(EnrichmentConfig(),
                                        hook=self.FakePgHook(fail=True))
        with pytest.raises(DependencyError):
            lookup.fetch(["CUS-1"])

    def test_rows_are_keyed_by_customer_id(self):
        rows = [("CUS-1", "Olivia Martin", "1988-04-14", "GA", "2018-06-05",
                 "ACTIVE")]
        found = PostgresCustomerLookup(
            EnrichmentConfig(), hook=self.FakePgHook(rows=rows)).fetch(["CUS-1"])
        assert found["CUS-1"]["customer_name"] == "Olivia Martin"

    def test_no_ids_means_no_query(self):
        hook = self.FakePgHook()
        assert PostgresCustomerLookup(EnrichmentConfig(), hook=hook).fetch([]) == {}
        assert hook.sql is None

    @pytest.mark.parametrize("bad", ["customers; DROP TABLE x", "cust omers",
                                     "customers--", ""])
    def test_an_unsafe_table_name_in_configuration_is_refused(self, bad):
        with pytest.raises(ValueError, match="unsafe SQL identifier"):
            _assert_identifier(bad)

    @pytest.mark.parametrize("good", ["customers", "claims.customers",
                                      "customer_records"])
    def test_legitimate_table_names_pass(self, good):
        _assert_identifier(good)


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload=None, status=200):
        self.headers = {}
        self.auth = None
        self.calls = []
        self.payload = payload if payload is not None else {"policies": []}
        self.status = status

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return FakeResponse(self.payload, self.status)


class TestPolicyApiCredentials:
    """Requirements section 3: credentials from an Airflow connection.

    The regression guard for a shipped gap -- `policy_api_conn_id` was
    configuration that no code path read, so every request went out
    unauthenticated.
    """

    def test_a_bearer_token_from_the_connection_is_attached(self, monkeypatch):
        class Conn:
            login = None
            password = "token-value"
            host = None
            extra_dejson = {}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        session = FakeSession()
        lookup = PolicyApiLookup(EnrichmentConfig(), session=session)
        lookup._apply_connection(session)
        assert session.headers["Authorization"] == "Bearer token-value"

    def test_basic_auth_when_the_connection_carries_a_login(self, monkeypatch):
        class Conn:
            login = "svc-claims"
            password = "pw"
            host = None
            extra_dejson = {}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        session = FakeSession()
        PolicyApiLookup(EnrichmentConfig(), session=session)._apply_connection(session)
        assert session.auth == ("svc-claims", "pw")

    def test_extra_headers_are_merged(self, monkeypatch):
        """Supports an API-key header style of authentication."""
        class Conn:
            login = None
            password = None
            host = None
            extra_dejson = {"headers": {"X-API-Key": "abc123"}}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        session = FakeSession()
        PolicyApiLookup(EnrichmentConfig(), session=session)._apply_connection(session)
        assert session.headers["X-API-Key"] == "abc123"

    def test_the_connection_host_overrides_the_configured_base_url(self, monkeypatch):
        class Conn:
            login = None
            password = "t"
            host = "https://policy-api.qa.internal"
            extra_dejson = {}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        session = FakeSession()
        lookup = PolicyApiLookup(EnrichmentConfig(), session=session)
        lookup._apply_connection(session)
        assert lookup.base_url == "https://policy-api.qa.internal"

    def test_an_unresolvable_connection_is_a_dependency_error(self, monkeypatch):
        """Not a rejection: without policies no claim can be judged."""
        def boom(conn_id):
            raise RuntimeError("metadata database unreachable")

        monkeypatch.setattr("airflow.sdk.Connection.get", staticmethod(boom))
        session = FakeSession()
        lookup = PolicyApiLookup(EnrichmentConfig(), session=session)
        with pytest.raises(DependencyError, match="could not resolve"):
            lookup._apply_connection(session)

    def test_a_missing_connection_is_a_dependency_error(self, monkeypatch):
        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: None))
        session = FakeSession()
        lookup = PolicyApiLookup(EnrichmentConfig(), session=session)
        with pytest.raises(DependencyError, match="not defined"):
            lookup._apply_connection(session)

    def test_the_password_is_never_logged(self, monkeypatch, caplog):
        class Conn:
            login = "u"
            password = "SUPER-SECRET"
            host = None
            extra_dejson = {}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        session = FakeSession()
        with caplog.at_level("DEBUG"):
            PolicyApiLookup(EnrichmentConfig(),
                            session=session)._apply_connection(session)
        assert "SUPER-SECRET" not in caplog.text


class TestPolicyApiFetch:
    """Requirement 7: batched lookups, and retries that still fail honestly."""

    def test_policies_are_keyed_by_policy_id(self):
        session = FakeSession({"policies": [
            {"policy_id": "POL-1", "policy_status": "ACTIVE"}]})
        lookup = PolicyApiLookup(EnrichmentConfig(), session=session)
        assert lookup.fetch(["POL-1"])["POL-1"]["policy_status"] == "ACTIVE"

    def test_lookups_are_batched_not_one_request_per_policy(self):
        session = FakeSession({"policies": []})
        cfg = EnrichmentConfig()
        PolicyApiLookup(cfg, session=session).fetch([f"POL-{i}" for i in range(5)])
        assert len(session.calls) == 1

    def test_a_persistent_api_failure_raises_rather_than_returning_empty(self):
        """An empty dict would reject every claim for a missing policy."""
        session = FakeSession(status=500)
        lookup = PolicyApiLookup(EnrichmentConfig(), session=session,
                                 sleep=lambda s: None)
        with pytest.raises(DependencyError, match="failed after"):
            lookup.fetch(["POL-1"])

    def test_no_ids_means_no_request(self):
        session = FakeSession()
        assert PolicyApiLookup(EnrichmentConfig(), session=session).fetch([]) == {}
        assert session.calls == []
