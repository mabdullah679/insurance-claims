# `claims/publish/` — Kafka publishing

**Requirements 10-11 and the section 4 retry rules.** Publishes every
successful claim to `insurance.claims.processed`, or fails the DAG trying.

## The load-bearing idea: acknowledgement accounting

> "Require producer acknowledgments from Kafka before the DAG treats a message
> as published." · "Acknowledged Kafka count must equal the successful claim
> count."

`producer.send()` returning without raising means the message reached a
**buffer**. It says nothing about durability. So every send's future is
resolved, and only a future that resolves without error increments
`acknowledged`.

## Message contract — exactly ten fields

```
claim_id · customer_id · policy_id · claim_date · claim_amount
risk_score · processing_status · processing_timestamp · dag_run_id · schema_version
```

Enrichment columns like `customer_name` stay **out** of the topic. Consumers
were promised a schema, and quietly widening it is how a schema stops meaning
anything.

- `claim_id` is the **message key**, so events for one claim stay ordered
  within a partition.
- `claim_amount` is serialised as a **string** (`"1250.75"`), never a float: a
  consumer wanting exactness must not be handed binary floating point.
- `validate_message` runs before anything is sent. A registry would reject on
  the broker side after a network round trip, which is a worse place to find
  out.

## Delivery semantics

| Setting | Value | Why |
|---|---|---|
| `acks` | `all` | An unacknowledged message must not count as published |
| `enable_idempotence` | `True` | Makes the library's own retries safe: at-least-once without duplicates |
| `retries` | 3 | Section 4: three after the initial attempt |
| `batch_size` | 500 | Bounded batches, per the operational controls |

> **Use `kafka-python>=3.0`, never `kafka-python-ng`.** The `-ng` fork rejects
> `enable_idempotence` at construction with
> `AssertionError: Unrecognized configs`, which kills the publish task before
> it sends anything. `tests/test_kafka_publish.py::TestProducerConstruction`
> is the guard.

## Retry and failure

An outer loop re-sends **only the unacknowledged messages**, up to
`max_retries`, with a configured delay. After exhaustion:

1. Unpublishable events go to the dead-letter topic **if one is configured**.
2. A `PublishError` is raised, carrying the run's counts on `.metrics` so the
   failed, retried and dead-lettered numbers survive the exception.
3. The DAG fails, and the input file is **not** archived.

A DLQ preserves events; it does not rescue the run.

## Credentials

TLS and SASL come from the Airflow connection named by `KAFKA_CONN_ID`.
Environment variables remain as a local-development fallback, and the
connection wins where both are set. Nothing is ever logged.

## If you change this

Never count a message as published without resolving its future. Never make
`acknowledged` reach `attempted` on a path where the broker did not confirm —
that count is what gates the archive step.
