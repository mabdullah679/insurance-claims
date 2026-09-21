# `claims/common/` — outcomes, config, staging, reconciliation, gateways

Cross-cutting machinery. Read `outcomes.py` first; everything else assumes it.

## `outcomes.py` — the type system of the whole design

```python
Rejection(reason, detail, record, claim_id)   # a VALUE  -> error folder, run succeeds
ClaimsPipelineError(Exception)                # RAISED   -> task fails
├── InputFileError        file missing, empty, or unreadable
├── SchemaError           required column absent (file-level, not record-level)
├── DependencyError       PostgreSQL or the policy API could not answer
├── PublishError          Kafka exhausted its retries
└── ReconciliationError   the counts do not add up
```

`Rejection.claim_id` may be `None` — a row can be rejected precisely *because*
it has no claim ID, and it must still reach the error folder. Reconciliation
therefore counts rows, not IDs.

`StageResult` is what every stage returns: `records`, `rejections`, `metrics`.
Its `summary()` is XCom-safe — counts and reasons, never claim records.

## `reconcile.py` — the arithmetic that gates the archive

Three invariants, all from requirements section 4:

```
successful + rejected  ==  total input records
kafka_acknowledged     ==  successful claims
rejections_stored      ==  rejected count        (when the store task reported)
```

A violation raises `ReconciliationError`. This runs **before** `archive_input_file`,
because if the counts do not reconcile we do not know what happened to the
missing records, and archiving would move the only evidence out of the inbound
bucket.

`rejection_rate_breached()` is separate and is an **alert**, not a failure. A
bad day for source data is something to hear about promptly; it is not a
reason to fail a run that handled every record correctly.

Counts are **derived from what is on disk**, not passed through XCom. If a task
was re-run and produced different numbers, reconciliation sees the real ones.

## `staging.py` — run-scoped files

XCom stores task return values in the metadata database. Claim records are PII
and a daily file can carry far more rows than XCom is designed to hold. So
records move through `data/staging/dt=YYYY-MM-DD/` and XCom carries counts and
keys.

`Decimal` and `date` survive the JSON round trip through a custom
encoder/decoder. Letting a claim amount decay into `float` across a task
boundary would put binary floating point into a financial reconciliation.

JSONL rather than Parquet: a human debugging a failed run at 3am can read a
line of it without a pyarrow install.

## `config.py` — every tunable, one place

Environment-driven dataclasses: `S3Config`, `KafkaConfig`, `EnrichmentConfig`,
`AlertConfig`. **Secrets are deliberately not here** — connection *IDs* are,
and the credential is resolved at the point of use.

The values most likely to need setting:

| Variable | Default | |
|---|---|---|
| `CLAIMS_S3_BUCKET` | `claims-inbound` | |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | |
| `KAFKA_CONN_ID` | unset | Airflow connection carrying TLS/SASL |
| `KAFKA_CLAIMS_DLQ_TOPIC` | unset | DLQ off unless set |
| `KAFKA_MAX_RETRIES` | `3` | §4 |
| `CLAIMS_REJECTION_RATE_THRESHOLD` | `0.10` | §5, "initially 10 percent" |
| `CLAIMS_SLA_MINUTES` | `90` | becomes the DAG run timeout |
| `CLAIMS_RISK_SCORER` | unset | **set this when Q1 is answered** |

## `gateways.py` — the three external systems

Each adapter has one job beyond the I/O: translate a transport failure into
`DependencyError` or `InputFileError`, and an empty-but-successful answer into
an empty result.

| Gateway | Connection | Notes |
|---|---|---|
| `S3Gateway` | `CLAIMS_S3_CONN_ID` (`aws_default`) | Archive is copy-then-delete: if the delete fails the file is in both places, which is recoverable. The reverse is not. |
| `PostgresCustomerLookup` | `CLAIMS_PG_CONN_ID` | Parameterised `IN` list; the table name is validated as an identifier before it reaches SQL |
| `PolicyApiLookup` | `CLAIMS_POLICY_API_CONN_ID` | Bearer token, basic auth, or `extra.headers`. The connection's host overrides the configured base URL |

Airflow imports live **inside** methods, not at module top level, so the
`claims` package stays importable and testable without Airflow installed.
