# Running the pipeline locally

A throwaway stand-in for the four systems the pipeline talks to, so you can
watch a real claim travel end to end without touching any real infrastructure.

| Container | Stands in for | Port |
|---|---|---|
| MinIO | the S3 inbound / error / archive bucket | 19000 (console 19001) |
| PostgreSQL | the customer table and the daily summary | 15432 |
| Kafka | `insurance.claims.processed` | 19092 |
| policy-api | the client's policy REST API | 18000 |

Ports are deliberately unusual and every container is named `claimsdemo-*`, so
this cannot collide with anything else running on the machine.

## Run it

```bash
docker compose -f demo/docker-compose.yml up -d   # start the four services
./demo/setup.sh                                   # buckets, connections, today's file
./demo/run.sh                                     # run the pipeline, show what it did
```

First run pulls images, so give it a few minutes. After that a full run takes
about a minute.

To see the rejection path instead of the happy path:

```bash
./demo/setup.sh rejects
./demo/run.sh
```

Tear everything down:

```bash
docker compose -f demo/docker-compose.yml down -v
```

## What you should see

**Happy path** — five valid claims:

```
  OK   check_input_file         success
  ...
  OK   send_notification        success

  DAG RUN: success

  CLM-QA-20260911-001       1250.75    10  SUCCESS
  ... 5 message(s) from this run

  date=2026-09-11  input=5  successful=5  rejected=0  kafka_acked=5

  still in inbound   (empty)
  archived           ['archive/dt=20260911/claims_20260911.csv']
```

**Rejection path** — six claims, five of them bad:

```
  DAG RUN: success            <- note: SUCCESS, not failure

  1 message(s) from this run

  CLM-QA-20260911-006 ... CUSTOMER_NOT_FOUND ... no customer record for CUS-QA-9999
  CLM-QA-20260911-007 ... POLICY_NOT_FOUND ... no policy record for POL-QA-9999
  CLM-QA-20260911-008 ... MISSING_REQUIRED_FIELD ... empty required field(s): claim_amount
  CLM-QA-20260911-009 ... INVALID_CLAIM_AMOUNT ... 'not-a-number' is not a positive number
  CLM-QA-20260911-010 ... INVALID_CLAIM_AMOUNT ... '-500.00' is not a positive number

  date=2026-09-11  input=6  successful=1  rejected=5  kafka_acked=1
```

**That second run is the one worth pausing on.** Five of six claims were
rejected and the pipeline still reports success, because it did exactly what
it should: each bad claim is in the error folder with a reason a person can
act on, the good one reached Kafka, and the counts reconcile (1 + 5 = 6).

A *failure* is something different — the file never arrived, a database was
unreachable, Kafka would not accept the claims. Then the run fails, an alert
goes out, and the input file is deliberately left in place so the day can be
re-run once the problem is fixed.

## Talking points

**For business stakeholders**

- The pipeline runs unattended at 2am and processes the previous day.
- A rejected claim is a normal outcome, not an outage. It lands in the error
  folder with a plain-English reason for a data quality analyst to review.
- The daily summary table is the single number to trust: input, successful,
  rejected, and how many Kafka confirmed.
- Notifications are already addressed to the right audiences — operations on
  success, operations and data quality on rejections, engineering on failure.

**For engineers**

- Nothing in `claims/` imports Airflow, which is why 229 tests run with no
  broker and no database.
- Credentials come from Airflow connections. `demo/setup.sh` registers four;
  pointing at real systems means changing those connections and the
  environment variables in `demo/env.sh`, not the code.
- Claim records never pass through XCom. They move through date-partitioned
  staging files, so re-running a single task works.
- The archive step sits behind reconciliation on purpose: the inbound file is
  the only re-runnable copy of the day's claims.

## Two things to flag honestly

**Risk scores in this demo are placeholders.** The numbers you see (10, 9, 11,
16, 17) are not the client's model, and the task logs say so on every run. The
scoring rules have not been supplied. See `docs/OPEN-QUESTIONS.md` question 1.

**This is a demo stack, not a deployment.** It uses a local SQLite database for
Airflow's own bookkeeping and a single-node Kafka. It proves the pipeline's
logic and its integration points, not its production capacity.

## If something goes wrong

| Symptom | Cause |
|---|---|
| `Connection with ID aws_default not found` | `setup.sh` has not run since the Airflow database was reset. Re-run it. |
| `expected claims file ... does not exist` | A previous run archived the file. Re-run `./demo/setup.sh` to put a fresh one in the inbound folder. |
| A task sits in `up_for_retry` for minutes | The DAG's real retry delay is 5 minutes. Ctrl-C is safe; `run.sh` reports task states from the database regardless. |
| Port already in use | Something else is on a `1xxxx` port. Change it in `demo/docker-compose.yml` and `demo/env.sh`. |
