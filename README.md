# Daily Insurance Claims Processing

An Apache Airflow 3 pipeline that checks a daily claims file, validates every record, joins customer and policy data, scores risk, publishes good claims to Kafka, parks bad ones with a reason, reconciles the counts, and archives the input. Built to [the requirements PDF](assignment-instructions/airflow_insurance_claims_dag_requirements.pdf); every requirement is mapped to code and tests in [FINAL-REPORT.md](FINAL-REPORT.md).

```
S3 inbound CSV -> validate -> enrich (Postgres + policy API) -> score -> Kafka topic
                                   \-> rejected claims -> S3 errors/ (in parallel)
                  both paths -> reconcile -> daily summary (Postgres) + archive + notify
```

Not local by default: S3, PostgreSQL, the policy REST API and Kafka are the client's systems. The `demo/` stack stands in for all four with Docker containers, and the tests fake them.

All data in this repo is synthetic (the QA workbook's `*-QA-*` IDs). The risk score is a labelled placeholder, and notifications are written to the log only; see [design decisions](#design-decisions).

## Prerequisites

- Python **3.12** (Airflow 3.0.2 does not support 3.13+). The tests need nothing else.
- Docker Desktop, running, only for the demo stack. Host ports 19000, 19001, 15432, 19092 and 18000 free.
- Not needed on the host: Kafka, Postgres, S3 or Airflow installs; the demo runs them in containers or inside the venv.

## Run it

1. `docker compose -f demo/docker-compose.yml up -d` (first run pulls images; wait for four `healthy` containers in `docker ps --filter name=claimsdemo`)
2. `./demo/setup.sh` (builds `.venv`, creates the bucket, registers connections, uploads the 2026-09-11 claims file)
3. `./demo/run.sh` (runs the DAG once, prints task outcomes, Kafka messages, summary row, archive state)
4. Rejection scenario: `./demo/setup.sh rejects` then `./demo/run.sh`
5. Tear down: `docker compose -f demo/docker-compose.yml down -v`

Step-by-step walkthrough with expected output: [FOLLOW-THESE-STEPS.md](FOLLOW-THESE-STEPS.md). Demo stack detail: [demo/README.md](demo/README.md).

If it goes wrong: a port is already allocated means edit it in `demo/docker-compose.yml` and `demo/env.sh`; "Connection with ID aws_default not found" means re-run `./demo/setup.sh`. Full table: [troubleshooting](FOLLOW-THESE-STEPS.md#if-something-goes-wrong).

## What it does

| Situation | Result | Requirement |
|---|---|---|
| Valid claim | Enriched, scored, published to `insurance.claims.processed` | 6-11 |
| Bad amount, unknown customer/policy, missing field, duplicate ID | Written to the S3 error folder with a reason; **DAG still succeeds** | 3, 4, 6, 7, 12 |
| File missing, empty, wrong columns, DB/Kafka down | Task fails, input file left in place, failure alert | 2, 3, section 4 |
| Rejection rate above 10% (`CLAIMS_REJECTION_RATE_THRESHOLD`) | Extra rejection alert | section 5 |
| Counts reconcile | Daily summary row written, input archived | 13, 14 |

## Interface

| Item | Value |
|---|---|
| DAG id | `daily_insurance_claims_processing`, schedule `0 2 * * *` America/New_York, `max_active_runs=1` |
| Manual date | DAG run conf `{"processing_date": "YYYY-MM-DD"}` |
| Kafka topic | `insurance.claims.processed` (dead-letter: `insurance.claims.dlq`) |
| Config | Environment variables read in [claims/common/config.py](claims/common/config.py); credentials via Airflow connections |

## Tests

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/ -q      # 229 passed
```

No Docker or external services: every system is faked. Coverage: validation, enrichment, risk scoring, Kafka retries and idempotence, reconciliation arithmetic, notification content, the DAG graph contract, the QA fixtures in [qa/](qa/README.md), and an end-to-end pipeline run. CI ([ci.yml](.github/workflows/ci.yml)) runs the same command plus a gitleaks scan. The Docker demo is not run in CI.

## Layout

| Path | Role |
|---|---|
| [dags/insurance_claims_dag.py](dags/insurance_claims_dag.py) | Orchestration only: logical date, XCom, logging |
| [claims/common/](claims/common/) | Outcomes, config, staging, reconciliation, S3/Postgres/policy gateways |
| [claims/validate/](claims/validate/), [claims/enrich/](claims/enrich/), [claims/score/](claims/score/) | Validation, customer/policy join, risk score |
| [claims/publish/](claims/publish/), [claims/load/](claims/load/), [claims/notify/](claims/notify/) | Kafka, daily summary, alerts |
| [tests/](tests/) | 229 tests |
| [qa/](qa/README.md) | Failure-case CSV fixtures beyond the client workbook |
| [demo/](demo/README.md) | Docker stand-ins plus setup and run scripts |
| [assignment-instructions/](assignment-instructions/README.md) | Client-supplied requirements PDF, QA workbook, diagram |
| [docs/](docs/ARCHITECTURE.md) | Architecture, [open questions](docs/OPEN-QUESTIONS.md), per-component notes |
| [FINAL-REPORT.md](FINAL-REPORT.md) | Requirement-to-code-to-test map and what is unproven |

## Design decisions

- **Rejection is a value, failure is an exception.** The obvious alternative, raising on any bad claim, would page engineers for normal data-quality noise; catching dependency errors as rejections would hide outages as data loss.
- **Rejections are stored in parallel with publishing.** Running them after Kafka would lose the data-quality record whenever the broker is down.
- **Archive sits behind reconcile.** The inbound file is the only re-runnable copy; moving it early destroys the evidence needed to repair a run.
- **Claim data stays out of XCom.** Records are PII; they move through date-partitioned staging files, which also makes single-task re-runs work.
- **Risk score is a placeholder** ([claims/score/risk.py](claims/score/risk.py)). The client has not supplied the formula, so it will not reproduce the workbook's expected scores ([open question 1](docs/OPEN-QUESTIONS.md)).
- **Notifications only log.** `Notifier` is a protocol with a logging default in [claims/notify/alerts.py](claims/notify/alerts.py); no Slack, email or paging is wired.
