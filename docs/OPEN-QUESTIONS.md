# Open questions for the client

Reconciling `airflow_insurance_claims_dag_requirements.pdf` (8pp, 12 Sept 2026)
and `insurance_claims_qa_test_data_20260911.xlsx` against what a working
pipeline actually needs. Ordered by how much they block delivery.

**Q1 blocks QA sign-off. Everything else has a documented default in place and
can be confirmed rather than waited on.**

---

## Q1 — Risk scoring rules (BLOCKING)

**Requirement 9** says "calculate a risk score for every claim". The Kafka
field table calls `risk_score` a "calculated fraud or operational risk value".
Neither document says how it is calculated.

The QA workbook nevertheless pins exact expected values:

| Claim | Amount | Coverage | Type | Expected score |
|---|---:|---:|---|---:|
| CLM-QA-20260911-001 | 1,250.75 | 50,000 | AUTO | 18 |
| CLM-QA-20260911-002 | 4,899.00 | 350,000 | HOME | 35 |
| CLM-QA-20260911-003 | 725.40 | 75,000 | AUTO | 12 |
| CLM-QA-20260911-004 | 9,875.25 | 500,000 | HOME | 52 |
| CLM-QA-20260911-005 | 2,150.00 | 50,000 | RENTERS | 24 |

What we checked: the scores are monotonic in claim amount, but **not** in
amount ÷ coverage limit (2.5%, 1.4%, 1.0%, 2.0%, 4.3% against 18, 35, 12, 52,
24), and score ÷ amount is not constant. Five data points cannot determine a
formula over the eight available inputs. We have not guessed.

**What we need:**
1. The scoring inputs and their weights (or the model artifact, if it is one).
2. The output range — the QA data implies a 0–100 integer. Confirm.
3. Is scoring deterministic for a given claim? (We have assumed **yes** — a
   corrected re-run of the same date must republish identical scores, or the
   Kafka reconciliation is meaningless.)
4. Should a claim that cannot be scored be rejected, or published with a
   default? (We have assumed **published with a bounded default**; rejecting
   would drop claims for a reason the business never asked us to reject on.)

**Current state:** `claims/score/risk.py` ships `LinearBandScorer`, a
documented placeholder with invented weights. It is bounded, deterministic and
clearly labelled, and **it will not reproduce the table above** — the QA step-5
diff will show a `risk_score` mismatch until the real rules land. Swapping it
in is a one-line config change (`CLAIMS_RISK_SCORER=module:attribute`) and
touches nothing else.

---

## Q2 — Two fields in `Expected_Kafka` cannot be compared literally

`processing_timestamp` is the placeholder string `QA_RUN_TIME`, and
`dag_run_id` is hardcoded to `manual__2026-09-11_qa`. Airflow generates both
per run.

**Assumption:** the QA comparison excludes these two fields, asserting only
that they are present and well-formed. Confirm, or tell us the intended
normalisation.

---

## Q3 — Duplicate resolution

Requirement 4 says to check for duplicate claim IDs but not which copy wins.

**Assumption:** keep the **first** occurrence in file order, reject the rest
with `DUPLICATE_CLAIM_ID`. Deterministic across re-runs, and keeps one copy of
a genuinely duplicated claim in the stream rather than dropping both. Confirm —
the alternatives (last wins, reject all copies) are all defensible.

---

## Q4 — Claims dated outside the processing day

The requirements never say what to do with a claim whose `claim_date` is not
the previous calendar day.

**Assumption:** process it normally, count it, and log a warning.
Late-reported claims are a business reality and nothing in the requirements
asks us to reject them. Flag if these should instead be rejected or held.

---

## Q5 — Policy status and period

The QA fixture has every policy `ACTIVE` and unexpired, so the requirements
never had to say what an inactive one means.

**Assumption:** reject with `POLICY_NOT_ACTIVE` when status is not `ACTIVE`,
or when the claim date falls outside the policy's effective/expiration window.
Configurable off via `require_active_policy=False`.

Open: are there other acceptable statuses (`PENDING_RENEWAL`, `GRACE_PERIOD`)?
Should a claim against a lapsed policy be rejected, or published with a flag
for David Chen to review?

---

## Q6 — Dead-letter destination

Requirement: "Write unpublishable events to an approved dead-letter
destination **when configured**."

**Assumption:** a Kafka DLQ topic, off unless `KAFKA_CLAIMS_DLQ_TOPIC` is set.
The DAG still fails after retry exhaustion whether or not a DLQ exists — the
DLQ preserves the events, it does not rescue the run.

Confirm the destination (Kafka topic? S3 prefix? both?) and whether a
successful DLQ write should change the run's outcome. We have assumed **not**.

---

## Q7 — Notification transport

Section 5 names four audiences and their content, but no channel.

**Current state:** `claims/notify/alerts.py` builds correctly-addressed,
correctly-worded notifications and sends them through a `Notifier` protocol
whose default writes to the Airflow task log. Point it at Slack, PagerDuty,
SNS or SMTP without touching the DAG.

Also open: does the **recovery** notification (section 5, fourth row) fire
automatically on a successful re-run after a failure, or does an operator send
it? We have built the message but not auto-triggered it — detecting "this run
repaired a previous failure" needs a rule the requirements do not state.

---

## Q8 — Operational completion window

"The DAG exceeds its operational completion window" is an alert condition;
no duration is given.

**Assumption:** 90 minutes, via `CLAIMS_SLA_MINUTES`. Needs a real number.

---

## Q9 — Input file variations

Not covered by the single QA fixture:
- Is the filename **always** `claims_YYYYMMDD.csv`, or can a day arrive as
  several files / a `.csv.gz`?
- What if the file arrives **late** — does the DAG wait (a sensor), or fail
  immediately? Currently it **fails immediately**, which is the stricter
  reading of requirement 1 and is easy to change to a sensor with a timeout.
- Expected daily volume? It determines whether the current in-memory
  processing is appropriate or whether this needs to be chunked.

---

## Q10 — Archive and error-folder retention

Requirement 14 says archive the original; requirement 12 says store rejects
in the S3 error folder. Neither says for how long, or whether a re-run should
overwrite or version the previous attempt's outputs.

**Assumption:** date-partitioned keys, overwritten in place on a re-run, so a
corrected run replaces its predecessor rather than accumulating copies.
Lifecycle policy left to the S3 bucket configuration.
