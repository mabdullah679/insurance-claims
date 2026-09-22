# Follow these steps

Three commands to get the claims pipeline running on your machine and see it
process a real day's claims end to end.

---

## Before you start

You need two things installed:

| | Check it with | If missing |
|---|---|---|
| **Docker Desktop**, running | `docker info` | [docker.com/get-started](https://www.docker.com/get-started/) |
| **Python 3.12** | `python3.12 --version` | macOS: `brew install python@3.12`<br>Ubuntu: `sudo apt install python3.12 python3.12-venv` |

Python 3.13 will not work. Airflow 3.0.2 does not support it yet.

Then get the code:

```bash
git clone https://github.com/mabdullah679/insurance-claims.git
cd insurance-claims
```

---

## Step 1 — Start the supporting services

```bash
docker compose -f demo/docker-compose.yml up -d
```

This starts four containers standing in for the systems the pipeline talks to:
an S3-compatible store, a PostgreSQL database, a Kafka broker, and a small
stub of a policy API.

**First time takes a few minutes** while the images download. After that it is
seconds. You should see four containers reported as `Started`.

Check they are healthy:

```bash
docker ps --filter name=claimsdemo
```

All four should say `(healthy)`. If one says `starting`, wait fifteen seconds
and look again.

---

## Step 2 — Prepare the environment

```bash
./demo/setup.sh
```

This does everything needed before a run:

- builds the Python environment and installs dependencies (**a few minutes the
  first time**, instant afterwards)
- creates the storage bucket
- registers the four connections the pipeline uses for credentials
- uploads a day's claims file to the inbound folder

It ends with `Ready. Now run: ./demo/run.sh`.

---

## Step 3 — Run the pipeline

```bash
./demo/run.sh
```

Takes about a minute. It runs the real pipeline against the real services and
then reports what happened.

### What you should see

```
--- task outcomes ------------------------------------------
  OK   check_input_file         success
  OK   validate_claims          success
  OK   enrich_claims            success
  OK   calculate_risk_scores    success
  OK   publish_to_kafka         success
  OK   store_rejected_claims    success
  OK   reconcile_run            success
  OK   update_daily_summary     success
  OK   archive_input_file       success
  OK   send_notification        success

  DAG RUN: success

--- claims published to Kafka ------------------------------
  claim_id                   amount  risk  status
  CLM-QA-20260911-001       1250.75    10  SUCCESS
  CLM-QA-20260911-002       4899.00     9  SUCCESS
  CLM-QA-20260911-003        725.40    11  SUCCESS
  CLM-QA-20260911-004       9875.25    16  SUCCESS
  CLM-QA-20260911-005       2150.00    17  SUCCESS

  5 message(s) from this run

--- the daily summary table --------------------------------
  date=2026-09-11  input=5  successful=5  rejected=0  kafka_acked=5

--- was the input file archived? ---------------------------
  still in inbound   (empty)
  archived           ['archive/dt=20260911/claims_20260911.csv']
```

Five claims went in, five reached Kafka, the daily summary was written, and
the original file moved to the archive folder. That is a complete, healthy
morning.

---

## Now see what happens when claims are bad

This is the part worth understanding, because it is the behaviour most people
expect to be different.

```bash
./demo/setup.sh rejects
./demo/run.sh
```

This loads a file of six claims where five have problems. You will see:

```
  DAG RUN: success            <-- still SUCCESS

  1 message(s) from this run

  CLM-QA-20260911-006 ... CUSTOMER_NOT_FOUND ... no customer record for CUS-QA-9999
  CLM-QA-20260911-007 ... POLICY_NOT_FOUND ... no policy record for POL-QA-9999
  CLM-QA-20260911-008 ... MISSING_REQUIRED_FIELD ... empty required field(s): claim_amount
  CLM-QA-20260911-009 ... INVALID_CLAIM_AMOUNT ... 'not-a-number' is not a positive number
  CLM-QA-20260911-010 ... INVALID_CLAIM_AMOUNT ... '-500.00' is not a positive number

  date=2026-09-11  input=6  successful=1  rejected=5  kafka_acked=1
```

**Five of six claims were rejected and the run still reports success.** That
is correct and deliberate.

A **rejected claim** is normal business. Someone typed a letter into an amount
field, or sent a claim for a customer who is not in your database. The
pipeline puts that claim in an error folder with a reason a person can act on,
carries on with the good claims, and the numbers still add up: 1 published
plus 5 rejected equals the 6 that arrived.

A **failed run** is something else entirely: the file never arrived, a
database was unreachable, or Kafka would not accept the claims. Then the run
fails, your engineers are alerted, and the original file is deliberately left
where it was so the day can be re-run once the problem is fixed.

That distinction is why a morning with forty rejected claims is a success, and
a morning with a missing file is an incident.

---

## Run the automated tests

```bash
.venv/bin/python -m pytest tests/ -q
```

Expect **229 passed**. These need no Docker and no services; every external
system is simulated. They cover validation rules, the customer and policy
join, Kafka delivery and retries, the reconciliation arithmetic, and the
notification content.

---

## Stop everything

```bash
docker compose -f demo/docker-compose.yml down -v
```

Removes the containers and their data. Nothing is left behind on your machine
apart from the cloned folder.

---

## Two things to know before you plan a deployment

**1. The risk scores are placeholders.** The numbers you see (10, 9, 11, 16,
17) are not a real risk model, and the pipeline says so in its logs on every
run. How a risk score should be calculated was not specified in the
requirements, and we chose not to invent a formula your fraud team would rely
on. When you supply the scoring rules, connecting them is one line of
configuration and nothing else changes. See `docs/OPEN-QUESTIONS.md`
question 1.

**2. This demo stack is not a deployment.** It proves the pipeline's logic and
its integration points on one laptop. It uses a single-node Kafka and a local
file-based database for Airflow's own bookkeeping. Running against your real
S3, PostgreSQL, policy API and Kafka means changing connection settings, not
code. `FINAL-REPORT.md` lists exactly what has and has not been proven.

---

## If something goes wrong

| What you see | What to do |
|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop is not running. Start it and retry step 1. |
| `Python 3.12 is required and was not found` | Install it with the command in the table at the top. |
| `Connection with ID aws_default not found` | Run `./demo/setup.sh` again. |
| `expected claims file ... does not exist` | A previous run archived it, which is correct behaviour. Run `./demo/setup.sh` to place a fresh file. |
| A step sits still for several minutes | The pipeline waits five minutes before retrying a failed step, which is right in production. Ctrl-C is safe; the report still prints. |
| `port is already allocated` | Something else uses one of the `1xxxx` ports. Change it in `demo/docker-compose.yml` and `demo/env.sh`. |
| Four containers not `healthy` | `docker compose -f demo/docker-compose.yml logs` shows why. |

---

## Where to go next

| Document | What it covers |
|---|---|
| [README.md](README.md) | What the pipeline does each morning, in plain language |
| [demo/README.md](demo/README.md) | The demo stack in more detail, with talking points |
| [FINAL-REPORT.md](FINAL-REPORT.md) | Every requirement mapped to the code that implements it |
| [docs/OPEN-QUESTIONS.md](docs/OPEN-QUESTIONS.md) | Ten decisions we need confirmed |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | For your engineers: how the pieces fit together |
