"""Kafka publishing: acknowledgement accounting, retries, and the message contract.

The claim that matters: a message is "published" only when the BROKER said so.
`producer.send()` returning without raising means the message reached a buffer.
Every test here that counts acknowledgements is defending the requirement
"Acknowledged Kafka count must equal the successful claim count".
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from claims.common.config import KafkaConfig
from claims.common.outcomes import PublishError
from claims.publish import kafka_publish as kp
from tests.conftest import FakeProducer

TOPIC = "insurance.claims.processed"


def claim(n: int, **over):
    return {"claim_id": f"CLM-{n:03d}", "customer_id": f"CUS-{n:03d}",
            "policy_id": f"POL-{n:03d}", "claim_amount": Decimal("100.50"),
            "claim_date": date(2026, 9, 11), "risk_score": 20, **over}


def cfg(**over):
    """A KafkaConfig with test-friendly defaults that callers can override.

    Built as a dict first so a test can override any field -- passing the
    defaults positionally alongside **over collides on the shared keys.
    """
    base = dict(bootstrap="unused:9092", topic=TOPIC, schema_version="1.0",
                max_retries=3, retry_delay_seconds=0.0, batch_size=500)
    return KafkaConfig(**{**base, **over})


class TestMessageContract:
    """Requirements section 3: the ten fields, and the key."""

    def test_message_carries_exactly_the_contracted_fields(self):
        msg = kp.build_message(claim(1), dag_run_id="run-1", schema_version="1.0")
        assert set(msg) == set(kp.MESSAGE_FIELDS)

    def test_enrichment_fields_do_not_leak_into_the_topic(self):
        """Consumers were promised a schema; widening it silently breaks that."""
        msg = kp.build_message(
            claim(1, customer_name="Olivia Martin", customer_state="GA"),
            dag_run_id="run-1", schema_version="1.0")
        assert "customer_name" not in msg
        assert "customer_state" not in msg

    def test_claim_id_is_the_message_key(self):
        """Requirement: same claim -> same partition -> ordered events."""
        fake = FakeProducer()
        kp.run([claim(1)], dag_run_id="r", cfg=cfg(), producer=fake)
        key, _ = fake.sent[TOPIC][0]
        assert key == "CLM-001"

    def test_decimal_amount_serialises_as_string_not_float(self):
        msg = kp.build_message(claim(1), dag_run_id="r", schema_version="1.0")
        assert msg["claim_amount"] == "100.50"
        assert not isinstance(msg["claim_amount"], float)

    def test_date_serialises_as_iso(self):
        msg = kp.build_message(claim(1), dag_run_id="r", schema_version="1.0")
        assert msg["claim_date"] == "2026-09-11"

    def test_dag_run_id_and_schema_version_are_carried(self):
        msg = kp.build_message(claim(1), dag_run_id="manual__2026-09-11",
                               schema_version="2.1")
        assert msg["dag_run_id"] == "manual__2026-09-11"
        assert msg["schema_version"] == "2.1"

    def test_processing_status_is_success(self):
        msg = kp.build_message(claim(1), dag_run_id="r", schema_version="1.0")
        assert msg["processing_status"] == "SUCCESS"


class TestSchemaValidation:
    """"Every event must satisfy the registered message schema.\""""

    def test_missing_field_is_refused(self):
        msg = kp.build_message(claim(1), dag_run_id="r", schema_version="1.0")
        del msg["risk_score"]
        with pytest.raises(PublishError, match="risk_score"):
            kp.validate_message(msg)

    def test_empty_claim_id_is_refused(self):
        msg = kp.build_message(claim(1, claim_id=""), dag_run_id="r",
                               schema_version="1.0")
        with pytest.raises(PublishError, match="claim_id"):
            kp.validate_message(msg)

    def test_non_integer_risk_score_is_refused(self):
        msg = kp.build_message(claim(1, risk_score="high"), dag_run_id="r",
                               schema_version="1.0")
        with pytest.raises(PublishError, match="risk_score"):
            kp.validate_message(msg)

    def test_bad_message_is_refused_before_anything_is_sent(self):
        fake = FakeProducer()
        with pytest.raises(PublishError):
            kp.run([claim(1, risk_score=None)], dag_run_id="r", cfg=cfg(),
                   producer=fake)
        assert fake.sent == {}


class TestAcknowledgementAccounting:
    def test_all_acknowledged_reports_the_full_count(self):
        fake = FakeProducer()
        m = kp.run([claim(i) for i in range(5)], dag_run_id="r", cfg=cfg(),
                   producer=fake)
        assert m["attempted"] == 5
        assert m["acknowledged"] == 5
        assert m["failed"] == 0

    def test_empty_input_publishes_nothing_and_succeeds(self):
        m = kp.run([], dag_run_id="r", cfg=cfg(), producer=FakeProducer())
        assert m["attempted"] == 0 and m["acknowledged"] == 0

    def test_batching_covers_every_claim(self):
        fake = FakeProducer()
        m = kp.run([claim(i) for i in range(12)], dag_run_id="r",
                   cfg=cfg(batch_size=5), producer=fake)
        assert m["acknowledged"] == 12
        assert len(fake.messages(TOPIC)) == 12


class TestRetryThenFail:
    """Section 4: three retries, then fail the DAG."""

    def test_transient_failure_recovers_within_the_retry_budget(self, no_sleep):
        fake = FakeProducer(fail_times=2)          # attempts 1-2 fail, 3rd works
        m = kp.run([claim(1)], dag_run_id="r", cfg=cfg(), producer=fake,
                   sleep=no_sleep)
        assert m["acknowledged"] == 1
        assert m["retries"] == 2

    def test_permanent_failure_raises_after_retry_exhaustion(self, no_sleep):
        fake = FakeProducer(fail_claims={"CLM-001"})
        with pytest.raises(PublishError, match="not acknowledged"):
            kp.run([claim(1)], dag_run_id="r", cfg=cfg(), producer=fake,
                   sleep=no_sleep)

    def test_retry_count_honours_configuration(self, no_sleep):
        fake = FakeProducer(fail_claims={"CLM-001"})
        delays = []
        with pytest.raises(PublishError):
            kp.run([claim(1)], dag_run_id="r", cfg=cfg(max_retries=2),
                   producer=fake, sleep=delays.append)
        assert len(delays) == 2

    def test_failure_message_says_not_to_archive(self, no_sleep):
        """The operator's next action depends on this sentence being there."""
        fake = FakeProducer(fail_claims={"CLM-001"})
        with pytest.raises(PublishError, match="must NOT be archived"):
            kp.run([claim(1)], dag_run_id="r", cfg=cfg(), producer=fake,
                   sleep=no_sleep)

    def test_partial_failure_still_fails_the_whole_run(self, no_sleep):
        """4 of 5 acknowledged is not success: the counts would not reconcile."""
        fake = FakeProducer(fail_claims={"CLM-003"})
        with pytest.raises(PublishError):
            kp.run([claim(i) for i in range(5)], dag_run_id="r", cfg=cfg(),
                   producer=fake, sleep=no_sleep)

    def test_retry_delay_is_applied_between_attempts(self, no_sleep):
        fake = FakeProducer(fail_claims={"CLM-001"})
        delays = []
        with pytest.raises(PublishError):
            kp.run([claim(1)], dag_run_id="r",
                   cfg=cfg(retry_delay_seconds=7), producer=fake,
                   sleep=delays.append)
        assert delays == [7, 7, 7]


class TestFailureMetricsSurvive:
    """Section 5 requires failed, retried and dead-lettered counts as metrics.

    On the failure path `run()` raises, so a plain `return` cannot carry them.
    They used to exist only inside the exception's message text, which meant
    the numbers the requirements ask us to track were unrecoverable on exactly
    the runs that needed them.
    """

    def test_the_exception_carries_the_counts(self, no_sleep):
        fake = FakeProducer(fail_claims={"CLM-003"})
        with pytest.raises(PublishError) as excinfo:
            kp.run([claim(i) for i in range(5)], dag_run_id="r", cfg=cfg(),
                   producer=fake, sleep=no_sleep)

        metrics = getattr(excinfo.value, "metrics", None)
        assert metrics is not None, "failure counts were lost with the exception"
        assert metrics["attempted"] == 5
        assert metrics["failed"] == 1
        assert metrics["acknowledged"] == 4
        assert metrics["retries"] == 3

    def test_dead_letter_count_is_reported_on_the_exception(self, no_sleep):
        """The DLQ count travels with the failure, whatever its value.

        Note the fake fails a claim id on EVERY topic, the dead-letter topic
        included, so the acknowledged count here is 0 by construction. What
        matters for the requirement is that the field is present and reflects
        what the DLQ write actually reported, rather than being absent.
        """
        fake = FakeProducer(fail_claims={"CLM-001"})
        with pytest.raises(PublishError) as excinfo:
            kp.run([claim(1)], dag_run_id="r", cfg=cfg(dlq_topic="claims.dlq"),
                   producer=fake, sleep=no_sleep)
        metrics = excinfo.value.metrics
        assert "dead_lettered" in metrics
        assert metrics["dead_lettered"] == 0
        # The event itself still reached the DLQ topic.
        assert len(fake.messages("claims.dlq")) == 1

    def test_dead_letter_count_reflects_a_successful_dlq_write(self, no_sleep):
        """A DLQ write the broker accepts is counted.

        The primary send fails for CLM-001; the DLQ envelope is re-keyed by
        the caller's failure_reason, so this fake accepts it there.
        """
        class DlqAcceptingProducer(FakeProducer):
            def send(self, topic, key=None, value=None):
                if topic == "claims.dlq":
                    self.sent.setdefault(topic, []).append((key, value))
                    from tests.conftest import FakeFuture
                    return FakeFuture()
                return super().send(topic, key=key, value=value)

        fake = DlqAcceptingProducer(fail_claims={"CLM-001"})
        with pytest.raises(PublishError) as excinfo:
            kp.run([claim(1)], dag_run_id="r", cfg=cfg(dlq_topic="claims.dlq"),
                   producer=fake, sleep=no_sleep)
        assert excinfo.value.metrics["dead_lettered"] == 1

    def test_metrics_never_let_a_failed_publish_look_complete(self, no_sleep):
        """acknowledged must stay below attempted, so reconciliation still fails."""
        fake = FakeProducer(fail_claims={"CLM-002"})
        with pytest.raises(PublishError) as excinfo:
            kp.run([claim(i) for i in range(3)], dag_run_id="r", cfg=cfg(),
                   producer=fake, sleep=no_sleep)
        m = excinfo.value.metrics
        assert m["acknowledged"] < m["attempted"]


class TestDeadLetter:
    def test_unpublishable_events_go_to_the_dlq_when_configured(self, no_sleep):
        fake = FakeProducer(fail_claims={"CLM-001"})
        with pytest.raises(PublishError):
            kp.run([claim(1)], dag_run_id="r",
                   cfg=cfg(dlq_topic="claims.dlq"), producer=fake,
                   sleep=no_sleep)
        dlq = fake.messages("claims.dlq")
        assert len(dlq) == 1
        assert dlq[0]["claim_id"] == "CLM-001"
        assert "failure_reason" in dlq[0]

    def test_no_dlq_configured_still_raises(self, no_sleep):
        fake = FakeProducer(fail_claims={"CLM-001"})
        with pytest.raises(PublishError):
            kp.run([claim(1)], dag_run_id="r", cfg=cfg(dlq_topic=None),
                   producer=fake, sleep=no_sleep)
        assert "claims.dlq" not in fake.sent


class TestSecurityConfiguration:
    """Requirements section 3: TLS in transit, credentials from a connection.

    The credential used to come from environment variables only, which is not
    "an approved secret manager or Airflow connection".
    """

    def test_no_connection_and_no_env_means_no_security_kwargs(self, monkeypatch):
        """A local plaintext broker must stay usable without a second path."""
        for var in ("KAFKA_SECURITY_PROTOCOL", "KAFKA_SASL_USERNAME",
                    "KAFKA_SSL_CAFILE"):
            monkeypatch.delenv(var, raising=False)
        assert kp._security_config(None) == {}

    def test_environment_still_works_for_local_development(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
        monkeypatch.setenv("KAFKA_SSL_CAFILE", "/certs/ca.pem")
        cfg_out = kp._security_config(None)
        assert cfg_out["security_protocol"] == "SSL"
        assert cfg_out["ssl_cafile"] == "/certs/ca.pem"

    def test_connection_credentials_are_used(self, monkeypatch):
        """The requirement: the secret comes from the connection."""
        class Conn:
            login = "claims-producer"
            password = "s3cret"
            extra_dejson = {"security_protocol": "SASL_SSL",
                            "sasl_mechanism": "SCRAM-SHA-512"}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        out = kp._security_config("kafka_claims")
        assert out["sasl_plain_username"] == "claims-producer"
        assert out["sasl_plain_password"] == "s3cret"
        assert out["sasl_mechanism"] == "SCRAM-SHA-512"
        assert out["security_protocol"] == "SASL_SSL"

    def test_connection_overrides_the_environment(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "from-env")

        class Conn:
            login = "from-connection"
            password = "p"
            extra_dejson = {}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        assert kp._security_config("kafka_claims")["sasl_plain_username"] == \
            "from-connection"

    def test_a_login_implies_tls_rather_than_plaintext(self, monkeypatch):
        """Never send a SASL password over an unencrypted connection."""
        class Conn:
            login = "u"
            password = "p"
            extra_dejson = {}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        assert kp._security_config("kafka_claims")["security_protocol"] == "SASL_SSL"

    def test_a_missing_connection_fails_loudly(self, monkeypatch):
        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: None))
        with pytest.raises(PublishError, match="not defined"):
            kp._security_config("kafka_claims")

    def test_an_unresolvable_connection_fails_loudly(self, monkeypatch):
        def boom(conn_id):
            raise RuntimeError("metadata database unreachable")

        monkeypatch.setattr("airflow.sdk.Connection.get", staticmethod(boom))
        with pytest.raises(PublishError, match="could not resolve"):
            kp._security_config("kafka_claims")

    def test_the_password_is_never_logged(self, monkeypatch, caplog):
        class Conn:
            login = "u"
            password = "TOP-SECRET-VALUE"
            extra_dejson = {}

        monkeypatch.setattr("airflow.sdk.Connection.get",
                            staticmethod(lambda conn_id: Conn()))
        with caplog.at_level("DEBUG"):
            kp._security_config("kafka_claims")
        assert "TOP-SECRET-VALUE" not in caplog.text


class TestProducerConstruction:
    """The installed Kafka client must ACCEPT the delivery semantics we ask for.

    This class exists because of a shipped defect: `requirements.txt` pinned
    `kafka-python-ng==2.2.3`, whose KafkaProducer raises
    `AssertionError: Unrecognized configs: {'enable_idempotence': True}` at
    construction. Every other test in this file injects a fake producer, so
    the whole suite stayed green while `publish_to_kafka` could not build a
    real producer at all. These tests touch the installed library.
    """

    def test_kwargs_carry_the_required_delivery_semantics(self):
        """Requirements section 3: acks before published, idempotent producer."""
        kwargs = kp.ClaimsProducer(cfg()).producer_kwargs()
        assert kwargs["acks"] == "all"
        assert kwargs["enable_idempotence"] is True
        assert kwargs["retries"] == 3

    def test_installed_client_accepts_every_kwarg_we_pass(self):
        """The regression guard. Fails against kafka-python-ng.

        Checked against the client library's own config schema rather than by
        connecting, so it runs anywhere with no broker.
        """
        kafka = pytest.importorskip("kafka")
        from kafka import KafkaProducer

        kwargs = kp.ClaimsProducer(cfg()).producer_kwargs()
        unsupported = [k for k in kwargs if k not in KafkaProducer.DEFAULT_CONFIG]
        assert not unsupported, (
            f"the installed Kafka client ({kafka.__version__}) rejects "
            f"{unsupported}; publish_to_kafka will die at construction. "
            f"Use kafka-python>=3.0, not kafka-python-ng.")

    def test_real_producer_constructs(self):
        """Builds the actual KafkaProducer. No broker needed.

        `api_version` is pinned so the constructor does not try to negotiate
        with a broker; everything before that point is the config validation
        that used to raise.
        """
        pytest.importorskip("kafka")
        from kafka import KafkaProducer

        kwargs = {**kp.ClaimsProducer(cfg()).producer_kwargs(),
                  "api_version": (2, 5, 0)}
        producer = KafkaProducer(**kwargs)
        try:
            # acks="all" is normalised to -1 by the client.
            assert producer.config["acks"] == -1
            assert producer.config["enable_idempotence"] is True
        finally:
            producer.close(timeout=1)


class TestDeterminism:
    def test_same_input_produces_the_same_message(self):
        """A corrected re-run for a logical date must republish identical events."""
        ts = datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc)
        a = kp.build_message(claim(1), dag_run_id="r", schema_version="1.0",
                             processed_at=ts)
        b = kp.build_message(claim(1), dag_run_id="r", schema_version="1.0",
                             processed_at=ts)
        assert a == b
