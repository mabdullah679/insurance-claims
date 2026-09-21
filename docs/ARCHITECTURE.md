# Architecture

Context for engineers and coding agents working on this pipeline. Read this
before changing anything; the per-component notes are in `docs/components/`.

## What this is

One Airflow DAG, `daily_insurance_claims_processing`, that runs at 02:00
America/New_York and processes the **previous calendar day's** claims.

```
check_input ─▶ validate ─▶ enrich ─▶ score ─▶ publish_kafka ─┐
                                        │                    │
                                        └─▶ store_rejections ─┤
                                                              ▼
                                                        reconcile
                                                              │
                                        ┌─────────────────────┼───────────┐
                                        ▼                     ▼           ▼
                                 update_summary        archive_input    notify
```

## The one rule everything else follows

The requirements draw a single distinction and build the rest on it:

| | Meaning | How it is expressed | Effect |
|---|---|---|---|
| **Rejection** | A claim the business expects to see fail: bad amount, unknown customer, duplicate ID | A **value** — `Rejection`, carried in a `StageResult` | Written to the S3 error folder with a reason. **The DAG still succeeds.** |
| **Failure** | Platform work that could not complete: file missing, database down, Kafka exhausted | An **exception** — `ClaimsPipelineError` and its subclasses | The task fails, the input file is **not** archived, an alert goes out. |

A stage cannot express one as the other by accident, because they are
different kinds of thing in the type system. If you find yourself catching a
`DependencyError` and turning it into a `Rejection`, stop: that converts an
outage into silent data loss.

## Two structural decisions worth knowing

**`store_rejected_claims` runs in parallel with `publish_to_kafka`, not after
it.** These are two independent promises to two different people. A broker
outage must not also lose the data-quality record.

**`archive_input_file` sits behind `reconcile_run`.** The file in the inbound
bucket is the only re-runnable copy of the day's claims. It moves only once
every input record is accounted for and Kafka has acknowledged every
successful claim.

## Data handoff

XCom carries **counts and keys only**. Claim records are PII and move through
date-partitioned staging files:

```
data/staging/dt=YYYY-MM-DD/
  input.csv            the raw object as fetched from S3
  validated.jsonl      survived validation
  enriched.jsonl       joined to customer and policy
  scored.jsonl         risk score attached — what gets published
  rejections/*.jsonl   one file per producing stage
  metrics/*.json       kafka, rejections, totals, error
```

A consequence worth knowing: re-running one task by hand works without
touching XCom, because each task finds its input by date.

## Layout

| Path | What lives there |
|---|---|
| `dags/insurance_claims_dag.py` | Orchestration only: logical date, XCom, logging. No business logic. |
| `claims/common/` | Cross-cutting: outcomes, config, staging, reconciliation, gateways |
| `claims/validate/` | Requirements 1-4 |
| `claims/enrich/` | Requirements 6-8 |
| `claims/score/` | Requirement 9 — **placeholder, see the component note** |
| `claims/publish/` | Requirements 10-11 |
| `claims/load/` | Requirement 13 |
| `claims/notify/` | Requirement 15 and section 5 |
| `tests/` | 229 tests, no broker or database needed |
| `qa/fixtures/` | The failure cases the client workbook lacks |

Every module under `claims/` is importable without Airflow. That is what makes
the pipeline testable without a scheduler, and it is worth preserving: Airflow
imports live inside functions, not at module top level.

## Conventions

- **Money is `Decimal`, never `float`.** These values are summed into a daily
  total and compared against coverage limits.
- **Scoring must be deterministic.** A corrected re-run of the same date must
  republish identical values, or the Kafka reconciliation is meaningless.
- **Credentials come from Airflow connections**, never from code or logs.
- **Every stage returns a `StageResult`**, so reconciliation has per-stage
  numbers to check.

## Known gaps

See `FINAL-REPORT.md`. The one that blocks QA sign-off is **risk scoring**:
the client has not supplied the formula, so `claims/score/risk.py` ships a
clearly labelled placeholder that will not reproduce the workbook's expected
values.
