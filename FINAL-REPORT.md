# Final report: `insurance-claims` against the client requirements

**Scope.** A requirement-by-requirement comparison of this repository with the
three client artefacts in `assignment-instructions/`: the requirements PDF
(8 pages, prepared 12 September 2026), the QA workbook (6 tabs, processing
date 2026-09-11), and the people/impact diagram.

**Observed against.** The working tree on 2026-09-21. Environment: Python 3.12
venv, Apache Airflow 3.0.2, Task SDK 1.0.2, kafka-python 3.0.11. Every
statement below was checked this session, either by running it or by reading
the exact lines cited. Nothing is inherited from an earlier report.

**Update, same day — fixes applied.** This report was written first against
the as-found tree, then nine findings were fixed. Section 5 marks each one
FIXED with the proof that closed it. The two blockers, F1 and F2, were each
reproduced first, fixed, and then **mutation-proven**: the fix was reverted,
the new guard test was watched fail, and the fix restored. Test count went
from 138 to **229**. What remains open is F3 (blocked on you), F10, F13, and
the ☑️ rows, which need live systems.

Status legend:

| Mark | Meaning |
|---|---|
| ✅ | implemented and covered by a test in `tests/` |
| ☑️ | implemented; only provable against live S3 / PostgreSQL / Kafka, which were not available |
| ⚠️ | partial, or rests on a stated assumption the PRD does not settle |
| ❌ | not implemented |
| 🐞 | defect confirmed this session by reproduction |

---

## 1. Verdict

**As found.** The pipeline logic was complete and tested for 13 of the 15
workflow steps, but **it would not have run end-to-end**. Two defects surfaced
only under a real Airflow 3 worker and a real Kafka client, which is precisely
why the 138-test suite was green:

- **F1.** `send_notification` called a method the Airflow 3 Task SDK does not
  provide. It failed on every run, and being the DAG's only leaf task, every
  run would have ended FAILED even when every claim was published. The failure
  alert the PRD requires never sent either.
- **F2.** The pinned Kafka client rejected `enable_idempotence` at
  construction, so `publish_to_kafka` died before sending a single message.

**After the fixes in this pass.** Both blockers are closed and
mutation-proven. Seven further findings (F4, F5, F6, F7, F8, F9, F15) are
fixed, and the three modules that had no tests at all now have them. 229 tests
pass.

**What is still open:**

- **F3 — risk scoring is a placeholder, blocked on you.** Neither document
  defines the formula and five data points cannot determine one. We did not
  guess. One line of configuration plugs in the real scorer.
- **F10 — no schema registry.** Field validation runs in-process. Adequate for
  QA; the PRD's "managed schema" is not met.
- **F13 — QA step 5 needs normalisation rules** for three fields the workbook
  stores differently from the event.
- **Every ☑️ row below.** Nothing has run against live S3, PostgreSQL, a
  policy API or a Kafka broker. That boundary is unproven.

---

## 2. Is this directory independent of `avapuck-main`?

**Yes.** No import, path, or configuration value in `insurance-claims/`
resolves to the sibling directory. The DAG parses and the tests run with only
this directory on `sys.path` (each adds its own root:
`dags/insurance_claims_dag.py:56-58` and `tests/conftest.py:17-19`). The venv
has no dependency on the sibling.

There were five prose-only mentions of the sibling project. No code path read
any of them. All five are now removed so the client receives a self-contained
tree:

| File | Was | Now |
|---|---|---|
| `requirements.txt:1-2` | comment: "pinned to match the sibling Airflow 3 project (avapuck-main)" | comment: pinned to Airflow 3.0.2 and matching providers |
| `dags/insurance_claims_dag.py:39-41` | docstring citing `avapuck-main/docs/ORCHESTRATION-SPEC.md` §3 | same rule, no citation |
| `dags/insurance_claims_dag.py:93-94` | "the same chain the sibling project uses" | "a fallback chain that covers every trigger path" |
| `claims/common/staging.py:15` | "same as the sibling project's" | removed |
| `README.md:96-99` | link to `../avapuck-main/docs/ORCHESTRATION-SPEC.md` | same paragraph without the link |

Verification after the edits: `grep -rniI -e avapuck -e sibling` over the tree
(excluding the venv, caches, and the client's own PDF) returns nothing.

---

## 3. Requirement traceability

### 3.1 Schedule (PRD §1)

| PRD | Status | Where | Evidence |
|---|---|---|---|
| Run daily at 2:00 AM Eastern Time | ✅ | DAG:139-140, `schedule="0 2 * * *"`, `start_date` in `America/New_York` | Parsed under 3.0.2: timetable is `CronTriggerTimetable`; next `run_after` computed as 02:00-04:00 |
| Process data from the previous calendar day | ✅ (see N1) | `logical_day()` DAG:87-114 | For a run firing 2026-09-12 02:00 the function returned 2026-09-11 when called with the real SDK `DagRun` type |
| Allow manual reruns for a selected date | ✅ | DAG:100-104, run conf key `processing_date` | README documents the trigger command; no unit test (F12) |
| Do not process future dates | ✅ | `_guard_future` DAG:117-128 raises `ClaimsPipelineError` | no unit test (F12) |
| Allow only one active DAG run at a time | ✅ | DAG:142 `max_active_runs=1` | parsed value is 1 |

**N1.** The previous-day arithmetic is correct under Airflow 3.0.2 defaults,
where a cron string yields `CronTriggerTimetable` and `logical_date` equals
the fire time (`[scheduler] create_cron_data_intervals` defaults to `False`,
verified). On Airflow 2.x, or with that setting `True`, `logical_date` is the
previous tick and the DAG would process D-2. Pin the setting, or assert it at
deployment.

### 3.2 Input sources (PRD §1)

| PRD | Status | Where | Note |
|---|---|---|---|
| Claims CSV files from an S3 inbound bucket | ☑️ | `S3Gateway.read` `claims/common/gateways.py:47-66` | `S3Hook` via the `aws_default` connection |
| Customer information from PostgreSQL | ☑️ | `PostgresCustomerLookup.fetch` gateways.py:134-159 | one parameterised `IN` query per run |
| Policy information from a REST API | ☑️ | `PolicyApiLookup` gateways.py | Endpoint shape `GET {base}/policies?policy_ids=a,b` returning `{"policies": [...]}` is assumed; the PRD does not specify it. Credentials now resolve from the Airflow connection (F4 fixed) |
| Expected filename `claims_YYYYMMDD.csv` | ✅ | `S3Config.inbound_key` `claims/common/config.py:54-56` | resolves to `inbound/claims_20260911.csv` |

### 3.3 The 15 workflow steps (PRD §1)

| # | PRD | Status | Where | Proof |
|---|---|---|---|---|
| 1 | Confirm the expected claims file exists in S3 | ☑️ | task `check_input_file` DAG:148-168; `S3Gateway.exists` / `read` | missing object raises `InputFileError`, DAG fails |
| 2 | Validate that the file is not empty | ✅ | gateways.py:63-65; `claims/validate/records.py:61-64, 76-77` | `test_validate.py::TestFileLevelFailures`; `test_qa_fixtures.py::TestDagFailureFixtures` (empty and header-only fixtures) |
| 3 | Validate required columns | ✅ | `REQUIRED_COLUMNS` records.py:32-33; `_assert_required_columns` records.py:81-86 raises `SchemaError` | `test_missing_required_column_raises_not_rejects`, `test_schema_error_names_every_missing_column` |
| 4 | Check for duplicate claim IDs | ✅ | records.py:119-125, first occurrence kept, later ones rejected `DUPLICATE_CLAIM_ID` (assumption Q3) | `test_validate.py::TestDuplicates`; `test_qa_fixtures.py::test_duplicates_reject_two_and_keep_three` |
| 5 | Load valid claim data into a staging area | ✅ | `Staging.write_stage("validated")` `claims/common/staging.py:90-95`; DAG:180 | JSONL under `data/staging/dt=YYYY-MM-DD/` on the worker volume. The PRD does not define "staging area"; this is a file, not a database table |
| 6 | Retrieve matching customer records from PostgreSQL | ☑️ | `claims/enrich/join.py:82` via the gateway | no live test |
| 7 | Retrieve policy information from the policy API | ☑️ | join.py:83 via the gateway | Authenticated from the connection (F4 fixed); `tests/test_gateways.py::TestPolicyApiCredentials` |
| 8 | Join claims with customer and policy information | ✅ | `join.run` join.py:60-129 | `test_enrich.py` (15 tests), including outage-versus-absence |
| 9 | Calculate a risk score for every claim | ❌ | `LinearBandScorer` `claims/score/risk.py:77-129`, labelled placeholder | F3. Every claim does receive an integer 0-100; the values are not the client's |
| 10 | Separate successful and rejected claims | ✅ | `StageResult.records` / `.rejections` `claims/common/outcomes.py:135-166`; DAG fan-out :387-390 | every stage test asserts `ok + rejected == input` |
| 11 | Publish successful claims to Kafka | ✅ | `kafka_publish.run` | F2 fixed and mutation-proven; `test_kafka_publish.py` (38 tests) |
| 12 | Store rejected claims in the S3 error folder | ☑️ | task `store_rejected_claims` DAG:254-276; `write_rejections` gateways.py:68-99; `to_error_row` outcomes.py:66-72 adds `rejection_reason` and `rejection_detail` | key `errors/dt=YYYYMMDD/rejected_claims_YYYYMMDD.csv`, `replace=True` |
| 13 | Update the daily claims summary table | ☑️ | `summary.run` `claims/load/summary.py:55-78`, upsert keyed on `processing_date` | no test (F12) |
| 14 | Archive the original input file | ☑️ | task `archive_input_file` DAG:307-322, downstream of `reconcile_run` (DAG:396); copy-then-delete gateways.py:101-117 | graph verified by parse |
| 15 | Send a completion notification with processing and Kafka totals | ✅ | task `send_notification`; `alerts.success` | F1 fixed and mutation-proven; `tests/test_dag_contract.py::TestFailedTaskIntrospection`, `tests/test_notify.py` |

### 3.4 End-to-end workflow diagram and the "important distinction" (PRD §2, §4)

The PRD's central rule, "a properly stored rejected claim does not
automatically fail the DAG; a DAG failure is platform work that could not
complete", is the type system of `claims/common/outcomes.py`: a rejection is a
value (`Rejection`, lines 52-72) and a failure is an exception
(`ClaimsPipelineError` and subclasses, lines 77-132). The graph matches the
diagram: valid claims go to `publish_to_kafka`, rejected claims go to
`store_rejected_claims` in parallel (DAG:390), an unacknowledged publish retries
then raises, and success requires reconciliation (DAG:393-396).

| Diagram path | Proof |
|---|---|
| Rejected claim → S3 error folder with reason, DAG still succeeds | `test_pipeline_integration.py::TestRejectionsStillSucceed` |
| Valid claim → Kafka → acknowledged → DAG success | `TestHappyPath::test_five_claims_published_and_reconciled` |
| Not acknowledged → retry, then DAG failure | `test_kafka_publish.py::TestRetryThenFail`; `TestPlatformFailures::test_kafka_exhaustion_fails_the_run` |
| Dependency outage is a failure, never a wave of rejections | `test_enrich.py::TestOutageIsNotRejection` |

### 3.5 Kafka publishing (PRD §3)

| PRD | Status | Where | Proof |
|---|---|---|---|
| Publish to `insurance.claims.processed` | ✅ | config.py:77 default | integration tests read that topic from the fake producer |
| Claim ID as the Kafka message key | ✅ | kafka_publish.py:155 | `test_claim_id_is_the_message_key` |
| Require producer acknowledgments before treating a message as published | ✅ | `acks="all"` :126; every send future resolved :163-168 | `TestAcknowledgementAccounting`; `test_partial_failure_still_fails_the_whole_run` |
| At-least-once delivery with idempotent producer behaviour | ✅ | `enable_idempotence=True` in `producer_kwargs()` | Real producer constructed with `acks=-1` and idempotence on; `TestProducerConstruction` |
| Managed schema and explicit schema version | ⚠️ | `schema_version` default `"1.0"`; `validate_message` | F10 still open: no registry; in-process field check only |
| TLS in transit, approved authentication and authorisation | ✅ | `_security_config(conn_id)` reads the Airflow connection; env is a dev fallback | F5 fixed; `TestSecurityConfiguration` (8 tests), incl. password never logged |
| Ten required message fields | ✅ | `MESSAGE_FIELDS` :44-46; `build_message` :61-82 emits exactly those ten | `test_message_carries_exactly_the_contracted_fields`; `test_enrichment_fields_do_not_leak_into_the_topic` |
| Publish in bounded batches | ✅ | `batch_size` 500 config.py:82; `_batches` :172-174 | `test_batching_covers_every_claim` |
| Retry transient failures without duplicated outcomes | ✅ | outer loop re-sends only unacknowledged messages :213-229 | `test_transient_failure_recovers_within_the_retry_budget` |
| Stable message key and event identity | ✅ | key is `claim_id`; `build_message` is deterministic for a fixed `processed_at` | `TestDeterminism`; `processing_timestamp` is wall-clock per run (Q2) |
| Credentials in an approved secret manager or Airflow connection | ✅ | S3 `aws_default`; PostgreSQL `claims_postgres`; policy API `CLAIMS_POLICY_API_CONN_ID` (F4 fixed); Kafka `KAFKA_CONN_ID` (F5 fixed) | |
| Track attempted, acknowledged, failed, and retried messages | ✅ | metrics on success, and on `PublishError.metrics` for the failure path | F9 fixed; `TestFailureMetricsSurvive` |

### 3.6 Failure handling and data quality (PRD §4)

Failure requirements:

| PRD | Status | Where | Proof |
|---|---|---|---|
| Retry Kafka publishing up to three times after the initial attempt | ✅ | `max_retries` 3 config.py:80; loop :213 | `test_retry_delay_is_applied_between_attempts` observes exactly three delays |
| Configured retry delay, no uncontrolled retry storms | ✅ | `retry_delay_seconds` 5 config.py:81; `retries=0` on the Airflow task DAG:230-231 so budgets do not compound | library-level `retries=3` inside each attempt bounds the worst case at 16 sends per message |
| Do not archive the input file when a required publish fails | ✅ | archive is downstream of `reconcile_run`, which is `all_success` on `[publish, store]` DAG:393-396 | `test_kafka_failure_leaves_the_run_unreconciled` |
| After retry exhaustion, fail the DAG and send an operational alert | ✅ | `PublishError`; alert path in `notify` | F1 fixed; the alert now sends |
| Write unpublishable events to a dead-letter destination when configured | ✅ | `_dead_letter` :257-276, only when `KAFKA_CLAIMS_DLQ_TOPIC` is set | `TestDeadLetter` |
| Permit a corrected run for the same logical processing date | ✅ | staging overwritten staging.py:90-95; S3 `replace=True`; summary upsert summary.py:36-52; conf override | `test_rerun_overwrites_staging_rather_than_appending` |

Data-quality requirements:

| PRD | Status | Where | Proof |
|---|---|---|---|
| Every successful claim has one acknowledged Kafka outcome | ✅ | `claims/common/reconcile.py:88-92` | `test_reconcile.py::TestKafkaAcknowledgement` |
| Every rejected claim is stored with a rejection reason | ✅ | reconcile.py:94-98 checks the count the S3 write actually returned; `to_error_row` | `TestStoredRejections`; `test_every_rejected_record_carries_a_reason` |
| Successful + rejected = total input | ✅ | reconcile.py:81-86 | `TestLostRecords` (both directions) |
| Acknowledged Kafka count = successful claim count | ✅ | reconcile.py:88 | `test_ack_shortfall_raises`, `test_ack_excess_raises` |
| Duplicate claim IDs detected before publishing | ✅ | validation precedes publishing in the graph | `TestDuplicates` |
| Every event satisfies the registered message schema | ⚠️ | `validate_message` runs before any send :201-202 | `TestSchemaValidation`; "registered" is F10 |

Success criteria: each maps to a row above. "Archived only after required
work succeeds" is the graph shape. "The completion notification is sent" is
F1.

### 3.7 Monitoring and notifications (PRD §5)

Required metrics:

| PRD | Status | Where |
|---|---|---|
| Total input records | ✅ | `RunTotals.input_records`, derived from what is on disk, staging.py:153-179 |
| Successful and rejected claim counts | ✅ | `RunTotals.successful` / `.rejected` |
| Kafka attempted and acknowledged counts | ✅ | kafka metrics file, `kafka_attempted` / `kafka_acknowledged` |
| Kafka failure and retry counts | ✅ | persisted on both paths; the failure path carries them on the exception (F9 fixed) |
| Dead-letter count | ✅ | same mechanism (F9 fixed) |
| DAG duration and task-level duration | ⚠️ | DAG duration in the success note; task durations remain Airflow-native, nothing additional is emitted |
| Dependency availability and API response failures | ⚠️ | logged (gateways.py:209-210), not emitted as a metric |

Alert conditions:

| PRD | Status | Mechanism |
|---|---|---|
| The expected S3 input file is missing or empty | ✅ | `InputFileError` fails `check_input_file` |
| The required file schema is invalid | ✅ | `SchemaError` fails `validate_claims` |
| PostgreSQL or policy API enrichment cannot complete | ✅ | `DependencyError` fails `enrich_claims`; never becomes rejections |
| Kafka publishing fails after all configured attempts | ✅ | `PublishError` fails `publish_to_kafka` |
| Kafka acknowledgment count differs from the successful count | ✅ | `ReconciliationError` fails `reconcile_run` |
| Rejected-record rate exceeds the threshold, initially 10 percent | ✅ | `rejection_rate_breached` reconcile.py:112-120; `CLAIMS_REJECTION_RATE_THRESHOLD` default 0.10; escalates the rejection note alerts.py:99-119. `test_qa_fixtures.py::TestAlertThresholdFixture` uses the 80 percent fixture |
| The DAG exceeds its operational completion window | ✅ | `dagrun_timeout` = `CLAIMS_SLA_MINUTES` (90 min default); F6 fixed, `TestOperationalWindow` |

For every ✅ row above the condition is detected and fails the right task.
Whether a person hears about it is F1.

Notification table:

| PRD row | Audience | Status | Where |
|---|---|---|---|
| Success | Claims Operations | ✅ | `alerts.success` carries run date, total, successful, rejected, Kafka acknowledged, duration. Delivery fixed with F1 |
| Record rejections | Claims Operations and Data Quality | ✅ | `alerts.rejections` carries rejected count, error location, top five reasons; suppressed when zero |
| DAG failure | Data Engineering and Claims Operations | ✅ | All five required fields present: failed task, error summary, run ID, retry status, operational impact (F7 fixed via `record_failure`) |
| Recovery | Data Engineering, Claims Operations, Customer Service | ✅ | Fires when a run succeeds for a date with a recorded prior failure; marker consumed after sending (F8 fixed) |

Transport for all four is `LoggingNotifier`, which writes to the task log.
The PRD names audiences, not channels (Q7).

### 3.8 People and stakeholder matrix (PRD §6-7, diagram)

Informational; nothing to implement. The audiences map onto the
`AlertConfig` channels config.py:110-112, and the recovery note adds
`customer-service` (alerts.py:166) because the PRD puts Michael Brown on that
row. The diagram's "Emily Carter reviews rejected claims" edge is the rejection
notification going to Claims Operations.

---

## 4. QA workbook reconciliation

| Tab | Result |
|---|---|
| `QA Guide` | Processing date 2026-09-11, file `claims_20260911.csv`, 5 records, expected outcome SUCCESS, 0 rejections. Steps 1-4 reproduce in `test_pipeline_integration.py::TestHappyPath`. Step 5 reproduces only with `TableScorer` standing in for the real formula (F3) |
| `Claims_Input` | Identical to `qa/fixtures/claims_20260911_happy.csv` and `conftest.QA_CSV`: the same five IDs, amounts 1250.75 / 4899.00 / 725.40 / 9875.25 / 2150.00, all dated 2026-09-11 |
| `Customers_QA` | IDs, names, states, and status matched. Dates of birth and `customer_since` in `tests/conftest.py` were off by days for four of the five customers. **Fixed this session** to the decoded Excel serials (F11) and re-verified equal |
| `Policies_QA` | All seven columns match `conftest.QA_POLICIES` exactly, including effective and expiration dates |
| `Expected_Kafka` | Ten columns equal `MESSAGE_FIELDS`, same order. Literal match on `claim_id`, `customer_id`, `policy_id`, `processing_status` = `SUCCESS`, `schema_version` = `1.0`, and `dag_run_id` when the run is named `manual__2026-09-11_qa`. Needs a normalisation rule for `claim_date` (Excel serial 46276 vs ISO `2026-09-11`), `claim_amount` (numeric 4899 vs string `"4899.00"`), and `processing_timestamp` (`QA_RUN_TIME` vs wall clock). `risk_score` matches only through `TableScorer` (F3). See F13 |
| `Audit` | All eight checks reproduce: 5 input, 5 unique IDs, 5 customer fixtures, 5 policy fixtures, 5 expected events, 5 successful, 0 rejected, all dated 2026-09-11 |

---

## 5. Findings

Ordered by consequence. "Fix" is a proposal, not something done here, unless
the finding is marked FIXED.

**F1 · FIXED (was 🐞 blocker) · `send_notification` crashed on every run.**
`_failed_tasks` called `dag_run.get_task_instances()`. In Airflow 3.0.2 a
task's `ctx["dag_run"]` is `airflow.sdk.api.datamodels._generated.DagRun`, a
pydantic model with no such method; nothing in `airflow.sdk` defines one.
Because this task is the DAG's only leaf and runs `ALL_DONE`, it turned every
successful run into a failed one and meant the operational alert it exists to
send never sent.

*Reproduced* by calling the function with that exact type:
`AttributeError: 'DagRun' object has no attribute 'get_task_instances'`.
*Fixed* by using the supported Task SDK call,
`ti.get_task_states(dag_id=..., run_ids=[run_id])`, which returns
`{run_id: {task_id: state}}`; introspection failure is caught and degraded
rather than allowed to kill the alert.
*Mutation-proven*: the original implementation was restored and
`tests/test_dag_contract.py::TestFailedTaskIntrospection` went red with the
AttributeError, then the fix was restored and it went green. An AST check
confirms no executable call to the old method remains (the only textual
occurrence is the docstring explaining the defect). 7 new tests.

**F2 · FIXED (was 🐞 blocker) · The Kafka producer could not be constructed.**
`requirements.txt` pinned `kafka-python-ng==2.2.3`, whose
`KafkaProducer.DEFAULT_CONFIG` has no `enable_idempotence`. `publish_to_kafka`
died inside `ClaimsProducer.__enter__` before any send. The suite passed
because every test injects a fake producer. Dropping the flag was not an
option: PRD §3 mandates idempotent producer behaviour.

*Reproduced* against the client's exact install:
`AssertionError: Unrecognized configs: {'enable_idempotence': True}`.
*Fixed* by pinning `kafka-python==3.0.11`, which accepts every kwarg the
module passes and validates the idempotence/`acks=all` pairing itself.
*Proven* by constructing the real `KafkaProducer` through
`ClaimsProducer.producer_kwargs()` and reading back `acks=-1` (all) and
`enable_idempotence=True`.
*Mutation-proven*: reinstalling the old client turned the new guard test red
with a message naming the defect and the fix; reinstalling the new one turned
it green. 3 new tests, and the producer kwargs are now a named method so
configuration is assertable without opening a socket.

**F3 · OPEN — blocked on the client · Risk scoring is a placeholder.**
Unchanged, and correctly so. `LinearBandScorer` has invented weights and does
not reproduce the workbook's 18 / 35 / 12 / 52 / 24. Neither client document
defines the formula, and five points cannot determine one over eight inputs.
The placeholder is bounded, deterministic, labelled in code, warned about in
the task log, and flagged `is_placeholder` in run metrics.
`test_placeholder_does_not_reproduce_the_qa_scores` asserts the gap on
purpose. Swapping in the real scorer is one environment variable. See
`docs/OPEN-QUESTIONS.md` Q1 and `docs/components/score.md`.

**F4 · FIXED · The policy API was called without credentials.**
PRD §3 requires credentials in "an approved secret manager or Airflow
connection". `EnrichmentConfig.policy_api_conn_id` was configuration that no
code path read — a control that was never code — and `PolicyApiLookup` used a
bare `requests.Session()`.

*Fixed* by resolving the connection through `airflow.sdk.Connection.get` and
applying the credential: bearer token (`password` with no `login`), HTTP basic
(`login` + `password`), or `extra.headers` for an API-key style. The
connection's `host` overrides the configured base URL. An unresolvable or
missing connection raises `DependencyError` rather than silently proceeding
unauthenticated, because without policies no claim can be judged.
*Mutation-proven*: reverting `_apply_connection` to a no-op turned 6 of the 7
new credential tests red. One test asserts the password never reaches the logs.

**F5 · FIXED · Kafka TLS and SASL came only from environment variables.**
*Fixed* by adding `KafkaConfig.conn_id` (`KAFKA_CONN_ID`) and having
`_security_config` consult the Airflow connection, which wins over the
environment wherever both are set. `login` implies `SASL_SSL` by default, so a
SASL password is never sent over an unencrypted connection. The environment
path remains as a local-development fallback so a plaintext test broker needs
no second code path. 8 new tests, including one asserting the password is
never logged.

**F6 · FIXED · The operational completion window was configuration only.**
`AlertConfig.sla_minutes` / `CLAIMS_SLA_MINUTES` was read by nothing and the
DAG had no timeout, so the §5 alert condition "the DAG exceeds its operational
completion window" could not fire.
*Fixed* with `dagrun_timeout=timedelta(minutes=AlertConfig().sla_minutes)` on
the DAG — 1:30:00 by default, verified on the parsed DAG. This also stops an
overrunning run from blocking the next day's, which `max_active_runs=1` would
otherwise do. 2 new tests.

**F7 · FIXED · The failure notification was missing two of its five fields.**
PRD §5 row 3 requires failed task, error summary, run ID, retry status and
operational impact. Nothing ever wrote the `error` metric the notifier read,
so the summary was always the fallback "see task logs", and `try_number` was
an accepted parameter the DAG never passed.
*Fixed* with a `record_failure` callback on `on_failure_callback` for every
task, writing the exception message, type, task ID, try number and max tries
to the run's staging directory as the task dies. `alerts.failure` now renders
a retry-status line ("attempt 2 of 3", and "retries exhausted" when nothing
more will run). The callback never raises — it runs while a task is already
failing, and an exception there would mask the original error. 5 new tests.

**F8 · FIXED · The recovery notification was never sent.**
`alerts.recovery` was built with the correct audience and content; nothing
called it. The PRD does not define how to detect "this run repaired a previous
failure".
*Fixed* using a local, unambiguous signal: an error record for this processing
date that this run then completed successfully means exactly that. The marker
is consumed after sending so a later run does not re-announce it. Customer
Service is on the recipient list because the PRD puts them there. 3 new tests.

**F9 · FIXED · Kafka failure and dead-letter counts were never persisted.**
On the failure path `run()` raised before the DAG wrote the kafka metrics
file, so the failed, retried and dead-lettered counts PRD §5 requires existed
only inside the exception message.
*Fixed* by attaching `.metrics` to the `PublishError` and having the task
persist them before re-raising.
*Safety re-verified*: a run whose publish failed still fails reconciliation
(`acknowledged` stays below `successful`), so writing these metrics cannot let
a broken run archive its input file. 4 new tests.

**F10 · OPEN · No managed schema or registry.**
`validate_message` enforces field presence and types in-process before any
send. There is no Schema Registry client, and `schema_version` is a string the
producer asserts rather than one a registry issues. Adequate for QA; PRD §3
"managed schema" is not met. Deliberately left: wiring a registry needs your
registry URL, subject naming convention and compatibility mode.

**F11 · FIXED · The test fixture had drifted from `Customers_QA`.**
`conftest.QA_CUSTOMERS` dates were hand-typed approximations (CUS-QA-1001
`1988-04-12` / `2018-06-01` where the workbook's Excel serials decode to
`1988-04-14` / `2018-06-05`, and three others). Replaced with the decoded
serials and verified equal for all five customers. `customer_since` feeds the
scorer's tenure input, so this matters once a real scorer arrives.

**F12 · FIXED · Three modules had no tests at all.**
`claims/common/gateways.py`, `claims/notify/alerts.py` and the DAG module were
referenced by no test — and F1 lived in exactly that gap.
*Fixed* with three new files: `tests/test_dag_contract.py` (24 tests, against
the real Airflow SDK types), `tests/test_notify.py` (21), and
`tests/test_gateways.py` (31). `claims/load/summary.py` remains covered only
indirectly; its SQL needs a live PostgreSQL to prove.

**F13 · OPEN · QA step 5 needs explicit normalisation rules.**
The workbook stores `claim_date` as an Excel serial (46276) and `claim_amount`
as a number (4899); the event carries an ISO date and a decimal string
(`"4899.00"`, chosen so consumers never receive binary floating point).
`processing_timestamp` and `dag_run_id` are per-run values. Q2 covers the last
two; the first two belong in the same question. This is a comparison-procedure
decision, not a code change.

**F14 · FIXED · The venv did not match `requirements.txt`.**
Providers, the Kafka client and psycopg2 were absent, and pendulum and pytest
were the wrong versions; the suite passed only because those imports sit
inside methods. The venv is now installed from `requirements.txt`, and a clean
clone was proven to install and pass from the three README commands.

**F15 · FIXED · `tests/test_qa_fixtures.py:9` referenced a file that did not exist.**
`qa/README.md` now exists and documents all seven fixtures, the outcome each
proves, and the two scenarios no CSV can express.

## 6. Changes made in this pass (2026-09-21)

**Packaging**

1. Removed all five sibling-project mentions (section 2). Docstrings and one
   comment only; no executable line changed.
2. Added `assignment-instructions/` with byte-identical copies of the three
   client artefacts and an index README.
3. Added `.gitignore` (the venv is 204 MB; `data/` is runtime staging).
4. The project is its own git repository, rooted at `insurance-claims/`, with
   root-relative paths. The parent directory is a plain hub folder.

**Fixes** — F1, F2, F4, F5, F6, F7, F8, F9, F11, F14, F15. Each is described
with its proof in section 5.

**Tests** — 138 to 229. Three new files covering modules that previously had
none:

| File | Tests | Covers |
|---|---:|---|
| `tests/test_dag_contract.py` | 24 | DAG logic against the real Airflow 3 SDK types: failure introspection, date arithmetic, graph shape, SLA, failure capture |
| `tests/test_gateways.py` | 31 | S3, PostgreSQL and policy API adapters, including credential resolution |
| `tests/test_notify.py` | 21 | All four rows of the §5 notification table |

Plus 15 added to `tests/test_kafka_publish.py` for producer construction,
security configuration and failure-metric survival.

**Documentation**

- `README.md` rewritten for a non-specialist reader: what runs each morning,
  the rejection-versus-failure idea in plain words, who gets told what, and an
  honest "what is not finished".
- `docs/ARCHITECTURE.md` plus seven component notes in `docs/components/`,
  each short and self-contained so an engineer or a coding assistant can be
  pointed at one file.
- `qa/README.md` documenting all seven fixtures (this also closed F15).

**Verification after the changes**

- 229 tests pass; nothing skipped.
- The DAG parses: 10 tasks, `dagrun_timeout` 1:30:00, failure callback on
  every task.
- A clean `git clone` into a fresh directory installs and passes from the
  three README commands, on Python 3.12, with every pin resolving.
- Zero `avapuck` or `sibling` matches in the tree.

## 7. What was not done

- **F3, risk scoring.** Blocked on you. No formula was guessed.
- **F10, schema registry.** Needs your registry URL, subject naming
  convention and compatibility mode.
- **F13, QA normalisation rules.** A comparison-procedure decision, not code.
- **Nothing was run against live S3, PostgreSQL, a policy API, or a Kafka
  broker.** Every ☑️ row in section 3 is unproven at that boundary. The Kafka
  producer was constructed for real, but never connected to a broker.
- **`claims/load/summary.py` still has no direct test.** Its SQL needs a live
  PostgreSQL to prove; it is covered only indirectly.
- **No remote was configured and nothing was pushed.** That is yours to do.

## 8. Recommended order before the client runs this

1. Send the risk-scoring rules (Q1), set `CLAIMS_RISK_SCORER`, and replace
   `test_placeholder_does_not_reproduce_the_qa_scores` with a positive
   assertion.
2. Create the four Airflow connections: `aws_default`, `claims_postgres`,
   `claims_policy_api`, and one for Kafka pointed at by `KAFKA_CONN_ID`.
3. Confirm the policy API's real endpoint shape matches the assumed
   `GET {base}/policies?policy_ids=a,b`.
4. Pin `[scheduler] create_cron_data_intervals = False` (note N1), or the DAG
   will process the wrong day.
5. Answer Q2 and the F13 normalisation question so QA step 5 can be compared
   mechanically.
6. One end-to-end `airflow dags test` against the QA fixtures with local
   services. That is the moment the ☑️ rows become ✅.
