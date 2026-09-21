# `claims/notify/` and `claims/load/` — notifications and the summary table

## `notify/alerts.py` — requirement 15 and section 5

The point is not "send an email". It is that the right person gets the fact
they need: an engineer gets an actionable failure with a run ID, while the
customer service lead gets to know whether the claim status a customer is
asking about can be trusted.

| Outcome | Audience | Minimum content | Built by |
|---|---|---|---|
| Success | Claims Operations | run date, total, successful, rejected, Kafka acknowledged, duration | `success()` |
| Record rejections | Claims Ops + Data Quality | rejected count, error location, top reasons | `rejections()` |
| DAG failure | Data Engineering + Claims Ops | failed task, error summary, run ID, retry status, operational impact | `failure()` |
| Recovery | + Customer Service | recovered run ID, restored date, backlog status | `recovery()` |

### Behaviour worth knowing

- **A clean run sends no rejection notice.** `rejections()` returns `None`
  when nothing was rejected. An alert people learn to ignore is not an alert.
- **Severity escalates** to `WARNING` when the rejection rate breaches the
  configured threshold.
- **The failure note states operational impact in words**, not just a stack
  trace, and says the input file was not archived — the reader's next action
  depends on that sentence.
- **Recovery fires automatically** when a run succeeds for a date that has a
  recorded failure from a previous attempt. The marker is then consumed so it
  does not re-announce.

### Transport

`Notifier` is a protocol; the default `LoggingNotifier` writes to the Airflow
task log and records what it sent. Deliberately not a no-op — a silently
dropped alert is worse than no alert system. Point it at Slack, PagerDuty, SNS
or SMTP without touching the DAG. The requirements name audiences, not
channels (open question Q7).

## `load/summary.py` — requirement 13

One row per processing date in `daily_claims_summary`, written as an
**upsert**. The requirements explicitly permit a corrected run for the same
logical date; a plain `INSERT` would leave two contradictory rows for one day
and whichever the reporting layer happened to read would decide what the
operations manager sees.

Columns: input records, successful, rejected, Kafka acknowledged, retries,
dead-lettered, rejection rate, DAG run ID, updated timestamp.

The table name is validated as a SQL identifier before use, and the DDL is
`CREATE TABLE IF NOT EXISTS`, so a fresh environment bootstraps itself.
