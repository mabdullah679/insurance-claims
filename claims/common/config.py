"""Runtime configuration, read from the environment in one place.

Every tunable the requirements name is here rather than scattered as literals
across stages, because most of them are operational dials that change without
a code review: the retry count, the rejection-rate alert threshold, the S3
prefixes, the Kafka topic.

Secrets are deliberately NOT here. The requirements ask for credentials in "an
approved secret manager or Airflow connection" (section 3, operational
controls), so connection details are resolved through Airflow connection IDs
at the point of use, and this module carries only the ID to look up.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from e


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be a number, got {raw!r}") from e


@dataclass(frozen=True)
class S3Config:
    """Where claims arrive, where rejects go, where processed files are filed.

    The error and archive prefixes are separate buckets-or-prefixes by
    configuration, not by convention, so a deployment can put rejects in a
    bucket Jessica Patel can read without granting her the inbound bucket.
    """

    bucket: str = field(default_factory=lambda: os.environ.get("CLAIMS_S3_BUCKET", "claims-inbound"))
    inbound_prefix: str = field(default_factory=lambda: os.environ.get("CLAIMS_S3_INBOUND_PREFIX", "inbound"))
    error_prefix: str = field(default_factory=lambda: os.environ.get("CLAIMS_S3_ERROR_PREFIX", "errors"))
    archive_prefix: str = field(default_factory=lambda: os.environ.get("CLAIMS_S3_ARCHIVE_PREFIX", "archive"))
    conn_id: str = field(default_factory=lambda: os.environ.get("CLAIMS_S3_CONN_ID", "aws_default"))

    def inbound_key(self, yyyymmdd: str) -> str:
        """`claims_YYYYMMDD.csv` under the inbound prefix (requirements 1)."""
        return f"{self.inbound_prefix}/claims_{yyyymmdd}.csv"

    def error_key(self, yyyymmdd: str) -> str:
        return f"{self.error_prefix}/dt={yyyymmdd}/rejected_claims_{yyyymmdd}.csv"

    def archive_key(self, yyyymmdd: str) -> str:
        return f"{self.archive_prefix}/dt={yyyymmdd}/claims_{yyyymmdd}.csv"


@dataclass(frozen=True)
class KafkaConfig:
    """Topic, delivery semantics, and the retry budget.

    The defaults encode the requirements rather than library defaults:
    `acks=all` because an unacknowledged message must not count as published,
    idempotence because at-least-once delivery with retries would otherwise
    duplicate, and 3 retries because section 4 says three after the initial
    attempt.
    """

    bootstrap: str = field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    topic: str = field(default_factory=lambda: os.environ.get("KAFKA_CLAIMS_TOPIC", "insurance.claims.processed"))
    # Requirements section 3: credentials belong in an Airflow connection or
    # secret manager. Unset means no connection lookup, which is what keeps a
    # local plaintext broker usable in development.
    conn_id: str | None = field(default_factory=lambda: os.environ.get("KAFKA_CONN_ID") or None)
    dlq_topic: str | None = field(default_factory=lambda: os.environ.get("KAFKA_CLAIMS_DLQ_TOPIC") or None)
    schema_version: str = field(default_factory=lambda: os.environ.get("CLAIMS_SCHEMA_VERSION", "1.0"))
    max_retries: int = field(default_factory=lambda: _int("KAFKA_MAX_RETRIES", 3))
    retry_delay_seconds: float = field(default_factory=lambda: _float("KAFKA_RETRY_DELAY_SECONDS", 5.0))
    batch_size: int = field(default_factory=lambda: _int("KAFKA_PUBLISH_BATCH_SIZE", 500))
    request_timeout_ms: int = field(default_factory=lambda: _int("KAFKA_REQUEST_TIMEOUT_MS", 30000))


@dataclass(frozen=True)
class EnrichmentConfig:
    """The two external dependencies, and how patient we are with them."""

    postgres_conn_id: str = field(default_factory=lambda: os.environ.get("CLAIMS_PG_CONN_ID", "claims_postgres"))
    customer_table: str = field(default_factory=lambda: os.environ.get("CLAIMS_CUSTOMER_TABLE", "customers"))
    summary_table: str = field(default_factory=lambda: os.environ.get("CLAIMS_SUMMARY_TABLE", "daily_claims_summary"))
    policy_api_base: str = field(default_factory=lambda: os.environ.get("CLAIMS_POLICY_API_BASE", "https://policy-api.internal"))
    policy_api_conn_id: str = field(default_factory=lambda: os.environ.get("CLAIMS_POLICY_API_CONN_ID", "claims_policy_api"))
    policy_api_timeout: float = field(default_factory=lambda: _float("CLAIMS_POLICY_API_TIMEOUT", 15.0))
    policy_api_attempts: int = field(default_factory=lambda: _int("CLAIMS_POLICY_API_ATTEMPTS", 3))
    policy_api_batch_size: int = field(default_factory=lambda: _int("CLAIMS_POLICY_API_BATCH", 100))


@dataclass(frozen=True)
class AlertConfig:
    """Thresholds that turn a normal run into one somebody hears about.

    `rejection_rate_threshold` starts at 0.10 because section 5 says "initially
    10 percent" -- the word initially is why this is configuration.
    """

    rejection_rate_threshold: float = field(default_factory=lambda: _float("CLAIMS_REJECTION_RATE_THRESHOLD", 0.10))
    sla_minutes: int = field(default_factory=lambda: _int("CLAIMS_SLA_MINUTES", 90))
    success_channel: str = field(default_factory=lambda: os.environ.get("CLAIMS_ALERT_SUCCESS", "claims-operations"))
    rejection_channel: str = field(default_factory=lambda: os.environ.get("CLAIMS_ALERT_REJECTIONS", "claims-ops,data-quality"))
    failure_channel: str = field(default_factory=lambda: os.environ.get("CLAIMS_ALERT_FAILURE", "data-engineering,claims-operations"))


@dataclass(frozen=True)
class Config:
    """Everything, assembled once per task."""

    s3: S3Config = field(default_factory=S3Config)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    enrichment: EnrichmentConfig = field(default_factory=EnrichmentConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)

    @classmethod
    def load(cls) -> "Config":
        return cls()
