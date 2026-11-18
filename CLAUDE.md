# CLAUDE.md -- insurance-claims

Global working rules live in `~/.claude/CLAUDE.md` (verify before claiming, branch + PR, staging gate, no secrets in commits). They apply here; they are not copied.

## What this is
One Airflow 3.0.2 DAG (`daily_insurance_claims_processing`, `dags/insurance_claims_dag.py`) that validates, enriches, risk-scores, publishes to Kafka, reconciles and archives a daily claims CSV. Business logic is in `claims/` (no Airflow imports at module top level); the DAG file is orchestration only. Read `docs/ARCHITECTURE.md` before changing anything.

## Commands (run on 2026-11-18, all verified)
- Python **3.12 only** (Airflow 3.0.2 does not support 3.13+).
- Setup: `python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt`
- Tests: `.venv/bin/python -m pytest tests/ -q` -> 229 passed, no Docker or services needed.
- Local stack (needs Docker): `docker compose -f demo/docker-compose.yml up -d`, `./demo/setup.sh [rejects]`, `./demo/run.sh`, teardown `docker compose -f demo/docker-compose.yml down -v`.
- CI: `.github/workflows/ci.yml` runs the same pytest command plus a gitleaks history scan.

## Rules specific to this repo
- Rejection (a bad claim) is a **value** (`Rejection` in a `StageResult`) and the DAG still succeeds. Failure (file missing, DB/Kafka down) is an **exception** (`ClaimsPipelineError`). Never turn a `DependencyError` into a `Rejection`.
- Money is `Decimal`, never `float`. Scoring must be deterministic.
- Claim records never go through XCom (counts and keys only); they move via `data/staging/dt=.../` (gitignored).
- Credentials come from Airflow connections, never code or logs. The `claimsdemo*`/`claims` passwords in `demo/` are throwaway local-stack values.
- `claims/score/risk.py` is a **placeholder** (client never supplied the formula); do not describe it as a real model.
- The default notifier only logs; there is no Slack/email/PagerDuty sender.

## Do not touch
- `assignment-instructions/` (client-supplied PDF, workbook, diagram) and `qa/fixtures/` (asserted by `tests/test_qa_fixtures.py`).
- `FINAL-REPORT.md` is the requirement-to-code map; update it only when behaviour changes.
- Never commit `.venv/`, `data/`, `demo/airflow/`, `.env*`.

## Data
All data is synthetic (`CUS-QA-*`, `POL-QA-*`, `CLM-QA-*` IDs from the QA workbook). Keep it that way; add no real claimant data.
