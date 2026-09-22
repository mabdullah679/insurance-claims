# Daily insurance claims processing

An Apache Airflow pipeline that processes your claims file every morning:
it checks the file, validates every claim, fills in customer and policy
details, scores each claim for risk, and publishes the good ones to Kafka for
your downstream teams.

Built to `assignment-instructions/airflow_insurance_claims_dag_requirements.pdf`.

---

## Quick start

You need **Python 3.12** (Airflow 3.0.2 does not yet support 3.13).

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/ -q
```

You should see **229 passed**. That is the whole install — the tests need no
Kafka, no database and no S3 bucket, because every external system is
simulated.

**Before you deploy to a real environment, read [FINAL-REPORT.md](FINAL-REPORT.md).**
It lists what is finished, what is not, and the one thing still blocked on
your side.

### See it actually run

To watch a claim travel end to end — S3 to Kafka, with a real database and a
real broker — there is a throwaway local stack in [demo/](demo/README.md):

```bash
docker compose -f demo/docker-compose.yml up -d
./demo/setup.sh
./demo/run.sh
```

It prints what each step did, the claims that reached Kafka, the rejected ones
with their reasons, the daily summary row, and whether the input file was
archived. `./demo/setup.sh rejects` shows the rejection path.

---

## What happens each morning

At **2:00 AM Eastern**, the pipeline processes **yesterday's** claims:

```
  ┌─ 1. Find the file ────────── claims_YYYYMMDD.csv in your S3 inbound bucket
  │
  ├─ 2. Check every claim ────── required fields present? amount a real number?
  │                              duplicate claim IDs?
  │
  ├─ 3. Fill in the details ──── customer record from PostgreSQL
  │                              policy details from your policy API
  │
  ├─ 4. Score the risk ───────── ⚠️ placeholder — see "What is not finished"
  │
  ├─ 5. Split the results ────── good claims ──▶ Kafka (insurance.claims.processed)
  │                              bad claims  ──▶ S3 error folder, with the reason
  │
  ├─ 6. Check the maths ──────── every claim accounted for? every good claim
  │                              confirmed by Kafka? If not, stop here.
  │
  └─ 7. Finish up ────────────── update the daily summary table
                                 file away the original CSV
                                 send the completion notice
```

### The one idea worth understanding

There are two very different kinds of "something went wrong", and the pipeline
never confuses them.

**A rejected claim is normal business.** A claim with a typo in the amount, or
for a customer who is not in your database, gets written to the error folder
with a plain-English reason. Your data quality analyst reviews it. **The run
still succeeds**, because the pipeline did exactly what it should.

**A failed run means the pipeline could not do its job.** The file never
arrived, the database was down, or Kafka would not accept the claims. Your
engineer gets an alert, and — importantly — **the original file is left
exactly where it was**, so the day can simply be run again once the problem is
fixed.

This is why a morning with 40 rejected claims is a success, and a morning with
zero claims and a missing file is a failure.

### Who hears about what

| When | Who is told | What they get |
|---|---|---|
| Normal completion | Claims Operations | totals, how many published, how long it took |
| Claims were rejected | Claims Operations, Data Quality | how many, where to find them, the top reasons |
| The run failed | Data Engineering, Claims Operations | which step, the error, the run ID, what it means for the business |
| A failed day was fixed | the above, plus Customer Service | which day is now current again |

If more than **10%** of a day's claims are rejected, the rejection notice is
escalated — that usually means something changed upstream, not that 50 people
made the same typo.

---

## What is not finished

Two things, stated plainly.

**1. Risk scoring is a placeholder.** This is the one item blocked on your
side. Neither the requirements nor the QA workbook says how a risk score is
calculated, and the five example scores in the workbook are not enough to work
it out. Rather than guess at something your fraud team would rely on, the
pipeline ships an obvious placeholder that produces sensible-looking but
**wrong** numbers, and says so in the logs every time it runs.

When you send the scoring rules, plugging them in is one line of
configuration. Nothing else changes. See
[docs/OPEN-QUESTIONS.md](docs/OPEN-QUESTIONS.md) question 1.

**2. Nothing has been run against your real systems.** The pipeline has never
touched your S3 bucket, your database, your policy API or your Kafka broker,
because we do not have access to them. Everything is proven against simulated
versions that behave the way the real ones are documented to. The first run in
your environment is the real test.

There are also ten smaller questions in
[docs/OPEN-QUESTIONS.md](docs/OPEN-QUESTIONS.md) where the requirements left a
choice open. Each one has a sensible default already working, so none of them
blocks you — but each is a decision you may want to confirm.

---

## Connecting it to your systems

Everything is set through environment variables. Nothing is hard-coded, and no
password is ever stored in the code.

| Setting | Default | What it is |
|---|---|---|
| `CLAIMS_S3_BUCKET` | `claims-inbound` | where the daily file arrives |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | your Kafka brokers |
| `KAFKA_CLAIMS_TOPIC` | `insurance.claims.processed` | where good claims go |
| `KAFKA_CONN_ID` | unset | Airflow connection holding your Kafka credentials |
| `KAFKA_CLAIMS_DLQ_TOPIC` | unset | optional dead-letter topic |
| `CLAIMS_REJECTION_RATE_THRESHOLD` | `0.10` | when to escalate a rejection notice |
| `CLAIMS_SLA_MINUTES` | `90` | how long a run may take before it is stopped |
| `CLAIMS_RISK_SCORER` | unset | **set this once you send the scoring rules** |

Usernames and passwords for S3, PostgreSQL, the policy API and Kafka all come
from **Airflow connections**, which is where your security team expects them
to be.

## Re-running a particular day

```bash
airflow dags trigger daily_insurance_claims_processing \
  --conf '{"processing_date": "2026-09-11"}'
```

Safe to repeat: a re-run replaces that day's results rather than adding a
second copy. Future dates are refused.

---

## What is in this folder

| | |
|---|---|
| `dags/` | the pipeline definition Airflow reads |
| `claims/` | the processing logic, one folder per step |
| `tests/` | 229 automated checks |
| `qa/fixtures/` | sample files covering the good day and the bad days |
| `docs/` | notes for your engineers — see below |
| `assignment-instructions/` | your original requirements, workbook and diagram |
| `FINAL-REPORT.md` | every requirement, and where it is implemented |

### For your engineers and AI coding assistants

| Document | Purpose |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | how the pieces fit together, and the conventions to keep |
| [docs/components/dag.md](docs/components/dag.md) | scheduling, the ten tasks, failure capture |
| [docs/components/validate.md](docs/components/validate.md) | file and record checking |
| [docs/components/enrich.md](docs/components/enrich.md) | customer and policy lookup |
| [docs/components/score.md](docs/components/score.md) | risk scoring, and how to plug in the real model |
| [docs/components/publish.md](docs/components/publish.md) | Kafka delivery, retries, dead-lettering |
| [docs/components/notify.md](docs/components/notify.md) | notifications and the summary table |
| [docs/components/common.md](docs/components/common.md) | shared machinery, config, reconciliation |
| [docs/OPEN-QUESTIONS.md](docs/OPEN-QUESTIONS.md) | the ten decisions awaiting your confirmation |
| [qa/README.md](qa/README.md) | the test fixtures and what each proves |

Each is short and self-contained, so an assistant can be pointed at one file
for the component being worked on.

## Checking the pipeline loads

```bash
.venv/bin/python -c "
import importlib.util
s = importlib.util.spec_from_file_location('d', 'dags/insurance_claims_dag.py')
m = importlib.util.module_from_spec(s); s.loader.exec_module(m)
print(m.dag.dag_id, '-', len(m.dag.tasks), 'tasks')"
```

Expected output: `daily_insurance_claims_processing - 10 tasks`

Then point your Airflow instance at the `dags/` folder.
