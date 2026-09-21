# `dags/insurance_claims_dag.py` — orchestration

Thin tasks. Each `@task` owns logical date, XCom and logging, then calls a
`run()` in `claims/` that has no Airflow import. That split is what makes the
pipeline unit-testable without a scheduler.

## Schedule

| | |
|---|---|
| Cron | `0 2 * * *` in `America/New_York` |
| Processes | the **previous** calendar day |
| Concurrency | `max_active_runs=1` |
| Catchup | off |
| Run timeout | `CLAIMS_SLA_MINUTES` (default 90) |

### The date resolution, and why it is not one line

`logical_date` is not guaranteed in the Airflow 3 task context — a run
triggered from the UI does not supply it, while `airflow dags test` does. So
`logical_day()` resolves through a fallback chain, and a manual re-run conf
wins over all of it:

```bash
airflow dags trigger daily_insurance_claims_processing \
  --conf '{"processing_date": "2026-09-11"}'
```

Future dates are refused with a message naming the date, rather than producing
a confusing "file not found" three tasks later.

> **Deployment note.** The previous-day arithmetic assumes Airflow 3 defaults,
> where a cron string yields `CronTriggerTimetable` and `logical_date` equals
> the fire time. If `[scheduler] create_cron_data_intervals` is `True`,
> `logical_date` is the previous tick and the DAG would process D-2. Pin that
> setting.

## The ten tasks

| Task | Requirement | Notes |
|---|---|---|
| `check_input_file` | 1-2 | Missing or empty raises; no partial success is available |
| `validate_claims` | 3-5 | |
| `enrich_claims` | 6-8 | 30-minute execution timeout |
| `calculate_risk_scores` | 9 | Warns loudly while the scorer is a placeholder |
| `publish_to_kafka` | 10-11 | `retries=0` — the publisher owns its own retry budget |
| `store_rejected_claims` | 12 | `ALL_DONE`, so rejects are filed even when publishing failed |
| `reconcile_run` | §4 | **The gate.** Everything after is safe only because this raised or did not |
| `update_daily_summary` | 13 | |
| `archive_input_file` | 14 | Only downstream of reconciliation |
| `send_notification` | 15, §5 | `ALL_DONE`, so it reports failures too |

### Why `publish_to_kafka` has `retries=0`

The publisher owns one authoritative retry loop and knows which individual
messages went unacknowledged. An Airflow-level retry would re-send the whole
batch and re-run the counting.

### Why `send_notification` uses `get_task_states`

It must inspect sibling task states to decide between the success and failure
notification. In Airflow 3 the task context's `dag_run` is a pydantic data
model with **no** `get_task_instances()` method — calling it raises
`AttributeError`, and because this task is the DAG's only leaf, that turns
every run into a failure while sending no alert at all. The supported route is
`ti.get_task_states(dag_id=..., run_ids=[run_id])`, which returns
`{run_id: {task_id: state}}`.

`tests/test_dag_contract.py` guards this against the real SDK type.

## Failure capture

`record_failure` is attached as `on_failure_callback` to every task. It writes
the exception message, task ID, try number and max tries into the run's
staging directory, because `send_notification` runs in a different process and
cannot see the traceback. It never raises — it runs while a task is already
failing, and an exception there would mask the original error.

## Testing the DAG itself

```bash
.venv/bin/python -m pytest tests/test_dag_contract.py -v   # needs Airflow
.venv/bin/python -m pytest tests/ -q --ignore=tests/test_dag_contract.py
```

The second command is the rest of the suite, which needs no Airflow at all.
