# QA fixtures

The client's workbook supplies one scenario: the happy path. The requirements
spend most of their text on what happens when things go wrong, so these
fixtures cover those cases too. Every one is asserted in
`tests/test_qa_fixtures.py` — a fixture nobody asserts against is just a file.

All fixtures use processing date **2026-09-11** and the customer and policy
fixtures in `tests/conftest.py`, which reproduce the workbook's
`Customers_QA` and `Policies_QA` tabs exactly.

## The fixtures

| File | Rows | Outcome | Requirement |
|---|---:|---|---|
| `claims_20260911_happy.csv` | 5 | 5 published, 0 rejected, **DAG succeeds** | The workbook's own scenario |
| `claims_20260911_duplicates.csv` | 5 | 3 published, 2 rejected, **DAG succeeds** | 4 — duplicate claim IDs |
| `claims_20260911_mixed_rejections.csv` | 6 | several reasons at once, reconciles, **DAG succeeds** | 3, 6, 7 |
| `claims_20260911_high_rejection_rate.csv` | 5 | 1 published, 4 rejected (80%), alert fires, **DAG succeeds** | §5 — 10% threshold |
| `claims_20260911_missing_column.csv` | 2 | `SchemaError`, **DAG fails** | 3 — required columns |
| `claims_20260911_empty.csv` | 0 | `InputFileError`, **DAG fails** | 2 — file not empty |
| `claims_20260911_header_only.csv` | 0 | `InputFileError`, **DAG fails** | 2 |

## The distinction these fixtures exist to prove

The first four **succeed**. Rejected records are a handled business outcome:
they go to the S3 error folder with a reason and the run is still a good run.
The last three **fail the DAG**, because no row in the file can be processed.

Getting this backwards is the most expensive mistake available here. Failing
the DAG on a bad record pages an engineer at 2am for a typo. Rejecting every
row of a broken export reports a successful run with a 100% rejection rate and
archives the input file.

## What has no fixture

**Kafka retry exhaustion.** It is a broker state, not a data state, so no CSV
can express it. It is exercised through a fake producer in
`tests/test_kafka_publish.py::TestRetryThenFail`.

**Dependency outage.** Same reason: PostgreSQL being down is not something a
claims file can describe. See `tests/test_enrich.py::TestOutageIsNotRejection`.

## Running them

```bash
.venv/bin/python -m pytest tests/test_qa_fixtures.py -v
```
