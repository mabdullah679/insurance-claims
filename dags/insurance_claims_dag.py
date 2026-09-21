"""Daily insurance claims processing -- the DAG.

    2:00 AM ET, previous calendar day, one active run at a time.

    check_input ─▶ validate ─▶ enrich ─▶ score ─▶ split
                                                   │
                        ┌──────────────────────────┴───────────┐
                        ▼                                      ▼
                  publish_kafka                        store_rejections
                        │                                      │
                        └──────────────┬───────────────────────┘
                                       ▼
                                  reconcile          ← gates everything after
                                       │
                          ┌────────────┼────────────┐
                          ▼            ▼            ▼
                  update_summary   archive_input  notify
                                   (only after reconcile passes)

WHY THE GRAPH HAS THIS SHAPE
----------------------------
`store_rejections` runs in PARALLEL with `publish_kafka`, not after it. A
Kafka outage must not prevent rejected claims from reaching the error folder --
those are two independent promises to two different people (Sarah Mitchell
gets published claims; Jessica Patel gets the rejects), and coupling them
would mean a broker problem also loses the data-quality record.

`archive_input` sits BEHIND `reconcile` because of requirement 4: "Do not
archive the original input file when a required downstream publish fails" and
"The original input file is archived only after required work succeeds". The
input file in the inbound bucket is the only re-runnable copy of the day's
claims; moving it before the counts add up would destroy the evidence needed
to repair the run.

`notify` uses a trigger rule that fires on success or failure, because a
completion notification that only arrives on good days is not a monitoring
system.

TASK GRANULARITY: each @task is thin orchestration -- logical date, XCom,
logging -- and calls a `run()` in `claims/` that has no Airflow import and is
unit-testable without a scheduler.

DATA HANDOFF: XCom carries COUNTS AND PATHS ONLY, never claim records. Claim
data moves through a run-scoped staging file on the shared volume, keyed by
logical date, so a re-run of one task finds its input without XCom and no PII
passes through the metadata database.
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pendulum
from airflow.sdk import DAG, task
from airflow.utils.trigger_rule import TriggerRule

from claims.common.config import AlertConfig, Config
from claims.common.outcomes import ClaimsPipelineError
from claims.common.reconcile import reconcile
from claims.common.staging import Staging

log = logging.getLogger("airflow.task")

TZ = pendulum.timezone("America/New_York")

def record_failure(context) -> None:
    """Persist the real exception so the failure alert can quote it.

    Requirements section 5 wants an "error summary" and "retry status" in the
    DAG-failure notification. `send_notification` runs in a separate process
    from the task that failed and cannot see its traceback, so the failing
    task writes the message to the run's staging directory on its way out and
    the notifier reads it back.

    This callback must never raise: it runs while a task is already failing,
    and an exception here would mask the original error.
    """
    try:
        ti = context.get("task_instance") or context.get("ti")
        day = logical_day(context)
        exc = context.get("exception")
        Staging(ROOT, day).write_metrics("error", {
            "message": str(exc) if exc else "see task logs",
            "exception_type": type(exc).__name__ if exc else None,
            "task_id": getattr(ti, "task_id", "unknown"),
            "try_number": getattr(ti, "try_number", None),
            "max_tries": getattr(ti, "max_tries", None),
        })
    except Exception as e:                      # pragma: no cover - defensive
        log.warning("could not record failure detail: %s", e)


DEFAULT_ARGS = {
    "owner": "data-engineering",
    # Retries cover transient infrastructure, not data problems: a rejected
    # record never reaches a retry, and a genuinely missing input file will
    # still be missing on attempt 3.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "depends_on_past": False,
    "email_on_failure": False,
    # Requirement section 5: the failure alert must carry an error summary and
    # retry status. Nothing else captures them.
    "on_failure_callback": record_failure,
}


def logical_day(ctx: dict):
    """The calendar day this run processes -- the day BEFORE the run date.

    `logical_date` is not guaranteed in the Airflow 3 task context: a run
    triggered from the UI or `airflow dags trigger` does not supply it, while
    `airflow dags test` does. Reading ctx["logical_date"] directly is a latent
    bug that only shows up on the UI path, so resolve through a fallback chain
    that covers every trigger path.

    A manual re-run for a specific date passes `{"processing_date":
    "2026-09-11"}` in the DAG run conf, which wins over the schedule -- that is
    requirement "allow manual reruns for a selected date".
    """
    conf = (ctx.get("dag_run") and getattr(ctx["dag_run"], "conf", None)) or {}
    if override := conf.get("processing_date"):
        d = pendulum.parse(str(override)).date()
        log.info("processing date overridden by run conf: %s", d)
        return _guard_future(d)

    ts = (ctx.get("logical_date")
          or ctx.get("data_interval_start")
          or (ctx.get("dag_run") and getattr(ctx["dag_run"], "logical_date", None))
          or (ctx.get("dag_run") and getattr(ctx["dag_run"], "run_after", None)))
    if ts is None:
        ts = pendulum.now(TZ)
    local = ts.in_timezone(TZ) if hasattr(ts, "in_timezone") else ts
    # The 02:00 run on day D processes day D-1 ("previous calendar day").
    return _guard_future((local - timedelta(days=1)).date())


def _guard_future(d):
    """Requirement: do not process future dates.

    Enforced rather than assumed: a mistyped manual re-run conf is the most
    likely way this happens, and the failure should name the date rather than
    producing a confusing "file not found" three tasks later.
    """
    today = pendulum.now(TZ).date()
    if d > today:
        raise ClaimsPipelineError(
            f"refusing to process future date {d} (today is {today} in {TZ.name})")
    return d


def run_id_of(ctx: dict) -> str:
    dr = ctx.get("dag_run")
    return getattr(dr, "run_id", None) or "unknown"


with DAG(
    dag_id="daily_insurance_claims_processing",
    description="Validate, enrich, score and publish daily insurance claims to Kafka",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=TZ),
    catchup=False,
    max_active_runs=1,
    # Section 5 alert condition: "the DAG exceeds its operational completion
    # window". Enforced rather than merely configured -- a run that overruns
    # is stopped and alerted on, instead of blocking tomorrow's run (which
    # max_active_runs=1 would otherwise do).
    dagrun_timeout=timedelta(minutes=AlertConfig().sla_minutes),
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["insurance", "claims", "kafka", "daily"],
) as dag:

    @task(task_id="check_input_file", execution_timeout=timedelta(minutes=10))
    def check_input(**ctx) -> dict:
        """Requirements 1-2: the expected file exists and is not empty.

        Both conditions raise rather than reject. There is no partial success
        available here -- without the file there is no run.
        """
        from claims.common.gateways import S3Gateway

        cfg = Config.load()
        day = logical_day(ctx)
        key = cfg.s3.inbound_key(day.strftime("%Y%m%d"))

        s3 = S3Gateway(cfg.s3)
        raw = s3.read(key)                      # raises InputFileError if absent/empty

        staging = Staging(ROOT, day)
        staging.write_raw(raw)
        log.info("input s3://%s/%s staged (%d bytes)", cfg.s3.bucket, key, len(raw))
        return {"key": key, "bucket": cfg.s3.bucket, "bytes": len(raw),
                "processing_date": day.isoformat()}

    @task(task_id="validate_claims")
    def validate_claims(**ctx) -> dict:
        """Requirements 3-5: columns, duplicates, then stage what is valid."""
        from claims.validate import records as validate_mod

        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        rows = validate_mod.parse_csv(staging.read_raw())   # raises SchemaError
        result = validate_mod.validate(rows, expected_date=day)

        staging.write_stage("validated", result.records)
        staging.write_rejections("validation", result.rejections)
        log.info("validated %d row(s): %d valid, %d rejected",
                 len(rows), result.ok_count, result.rejected_count)
        return result.summary()

    @task(task_id="enrich_claims", execution_timeout=timedelta(minutes=30))
    def enrich_claims(**ctx) -> dict:
        """Requirements 6-8: customer, policy, join.

        A dependency outage raises DependencyError from inside the gateways and
        fails the task. It must never become a wave of rejections.
        """
        from claims.common.gateways import PolicyApiLookup, PostgresCustomerLookup
        from claims.enrich import join

        cfg = Config.load()
        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        claims = staging.read_stage("validated")

        result = join.run(
            claims,
            customers=PostgresCustomerLookup(cfg.enrichment),
            policies=PolicyApiLookup(cfg.enrichment),
            as_of=day)

        staging.write_stage("enriched", result.records)
        staging.write_rejections("enrichment", result.rejections)
        log.info("enriched %d claim(s), %d rejected",
                 result.ok_count, result.rejected_count)
        return result.summary()

    @task(task_id="calculate_risk_scores")
    def score_claims(**ctx) -> dict:
        """Requirement 9. NOTE: the scorer is a placeholder until the client
        supplies the real rules -- see claims/score/risk.py."""
        from claims.score import risk

        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        result = risk.run(staging.read_stage("enriched"))
        staging.write_stage("scored", result.records)

        if result.metrics.get("is_placeholder"):
            log.warning("RISK SCORES ARE PLACEHOLDER VALUES and will not match "
                        "the QA workbook. Set CLAIMS_RISK_SCORER once the "
                        "client supplies the scoring rules.")
        return result.summary()

    @task(task_id="publish_to_kafka", execution_timeout=timedelta(minutes=45),
          retries=0)
    def publish_to_kafka(**ctx) -> dict:
        """Requirements 10-11 and the section 4 retry rules.

        `retries=0` at the Airflow level is deliberate: the publisher owns its
        own retry budget (3 after the initial attempt, per the requirements)
        and knows which individual messages were unacknowledged. An Airflow
        retry would re-send the whole batch and re-run the counting, which is
        both wasteful and harder to reason about than one authoritative
        retry loop.
        """
        from claims.publish import kafka_publish

        cfg = Config.load()
        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        claims = staging.read_stage("scored")

        try:
            metrics = kafka_publish.run(claims, dag_run_id=run_id_of(ctx),
                                        cfg=cfg.kafka)
        except ClaimsPipelineError as e:
            # Section 5 lists Kafka failure, retry and dead-letter counts as
            # required metrics. Persist whatever the publisher counted before
            # letting the failure propagate, or those numbers exist only
            # inside an exception message.
            if getattr(e, "metrics", None):
                staging.write_metrics("kafka", e.metrics)
                log.error("kafka publish failed: %s of %s acknowledged, "
                          "%s dead-lettered",
                          e.metrics.get("acknowledged"),
                          e.metrics.get("attempted"),
                          e.metrics.get("dead_lettered"))
            raise

        staging.write_metrics("kafka", metrics)
        return metrics

    @task(task_id="store_rejected_claims", trigger_rule=TriggerRule.ALL_DONE)
    def store_rejections(**ctx) -> dict:
        """Requirement 12: rejects to the S3 error folder, with reasons.

        ALL_DONE, so rejected claims are still filed when publishing fails.
        Jessica Patel's data-quality record does not depend on the broker
        being healthy -- and when the run is repaired and re-run, this
        overwrites the same key rather than accumulating duplicates.
        """
        from claims.common.gateways import S3Gateway

        cfg = Config.load()
        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        rows = staging.read_all_rejections()
        if not rows:
            log.info("no rejected claims for %s", day)
            return {"stored": 0, "key": None}

        key = cfg.s3.error_key(day.strftime("%Y%m%d"))
        stored = S3Gateway(cfg.s3).write_rejections(key, rows)
        staging.write_metrics("rejections", {"stored": stored, "key": key})
        return {"stored": stored, "key": f"s3://{cfg.s3.bucket}/{key}"}

    @task(task_id="reconcile_run")
    def reconcile_run(**ctx) -> dict:
        """The gate. Requirement: counts must reconcile before we call it done.

        Everything after this task -- archiving especially -- is only safe
        because this one raised or did not.
        """
        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        totals = staging.totals()
        rejections_metrics = staging.read_metrics("rejections")

        summary = reconcile(
            totals,
            rejections_stored=rejections_metrics.get("stored") if rejections_metrics else None)
        staging.write_metrics("totals", summary)
        return summary

    @task(task_id="update_daily_summary")
    def update_summary(**ctx) -> dict:
        """Requirement 13."""
        from claims.load import summary as summary_mod

        cfg = Config.load()
        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        return summary_mod.run(day, staging.totals(), dag_run_id=run_id_of(ctx),
                               cfg=cfg.enrichment)

    @task(task_id="archive_input_file")
    def archive_input(**ctx) -> dict:
        """Requirement 14 -- and only now.

        Reaching this task means reconcile_run did not raise, so every input
        record is accounted for and every successful claim was acknowledged by
        Kafka. That is the precondition the requirements put on archiving.
        """
        from claims.common.gateways import S3Gateway

        cfg = Config.load()
        day = logical_day(ctx)
        stamp = day.strftime("%Y%m%d")
        src, dest = cfg.s3.inbound_key(stamp), cfg.s3.archive_key(stamp)
        S3Gateway(cfg.s3).archive(src, dest)
        return {"archived_from": src, "archived_to": dest}

    @task(task_id="send_notification", trigger_rule=TriggerRule.ALL_DONE)
    def notify(**ctx) -> dict:
        """Requirement 15, plus the section 5 alert conditions.

        ALL_DONE so this reports failures too. It inspects the actual task
        states rather than assuming success, which is what lets one task serve
        both the success and failure rows of the notification table.
        """
        from claims.notify import alerts

        cfg = Config.load()
        day = logical_day(ctx)
        staging = Staging(ROOT, day)
        notifier = alerts.LoggingNotifier()

        failed = _failed_tasks(ctx)
        error_detail = staging.read_metrics("error") or {}
        if failed:
            # Requirement section 5, failure row: failed task, error summary,
            # run ID, retry status, operational impact. `record_failure` wrote
            # the error and retry fields as the task died.
            note = alerts.failure(
                day, failed_task=", ".join(failed),
                error=error_detail.get("message", "see task logs"),
                dag_run_id=run_id_of(ctx),
                try_number=error_detail.get("try_number"),
                max_tries=error_detail.get("max_tries"),
                cfg=cfg.alerts)
            notifier.send(note)
            return {"sent": 1, "kind": "failure", "failed_tasks": failed}

        totals = staging.totals()
        sent = 0
        notifier.send(alerts.success(day, totals, dag_run_id=run_id_of(ctx),
                                     duration_seconds=_duration(ctx),
                                     cfg=cfg.alerts))
        sent += 1
        rejection_note = alerts.rejections(
            day, totals,
            error_location=(staging.read_metrics("rejections") or {}).get("key", "n/a"),
            cfg=cfg.alerts)
        if rejection_note:
            notifier.send(rejection_note)
            sent += 1

        # Section 5, recovery row. An error record for this processing date
        # that this run went on to complete successfully means exactly one
        # thing: a previous attempt failed and this one repaired it. Customer
        # Service is on this list because Michael Brown fielded the calls
        # during the outage and needs to know claim status is current again.
        if error_detail:
            notifier.send(alerts.recovery(day, totals,
                                          dag_run_id=run_id_of(ctx),
                                          cfg=cfg.alerts))
            sent += 1
            # Consume the marker so tomorrow's run does not re-announce a
            # recovery that already happened.
            staging.write_metrics("error", {})
            log.info("recovery notification sent for %s (previous attempt "
                     "failed in task %s)", day, error_detail.get("task_id"))
        return {"sent": sent, "kind": "success", **totals.as_dict()}

    def _failed_tasks(ctx) -> list[str]:
        """Which sibling tasks failed, via the supported Task SDK call.

        NOT `dag_run.get_task_instances()`. In Airflow 3 a task's
        `ctx["dag_run"]` is `airflow.sdk.api.datamodels._generated.DagRun`, a
        pydantic model carrying data only -- it has no such method, and calling
        it raised AttributeError on EVERY run. Because this task is the DAG's
        only leaf and runs with ALL_DONE, that turned every successful run into
        a failed one and meant the operational alert this task exists to send
        never sent.

        `ti.get_task_states` is the Task SDK's supported route to sibling
        state. It returns {run_id: {task_id: state}}.
        """
        ti = ctx.get("ti") or ctx.get("task_instance")
        dr = ctx.get("dag_run")
        run_id = getattr(dr, "run_id", None)
        if ti is None or run_id is None:
            log.warning("cannot resolve task states: no task instance in context")
            return []

        try:
            states = ti.get_task_states(dag_id=dag.dag_id, run_ids=[run_id])
        except Exception as e:
            # Never let the notification task die over its own introspection:
            # a failed run whose alert also fails is the worst outcome here.
            log.warning("could not read sibling task states (%s); "
                        "reporting on staged metrics only", e)
            return []

        by_task = (states or {}).get(run_id, {}) or {}
        return sorted(
            task_id for task_id, state in by_task.items()
            if str(getattr(state, "value", state)).lower() == "failed"
            and task_id != "send_notification")

    def _duration(ctx) -> float | None:
        dr = ctx.get("dag_run")
        start = getattr(dr, "start_date", None) if dr else None
        return (time.time() - start.timestamp()) if start else None

    # ── The graph ─────────────────────────────────────────────────────────
    t_check = check_input()
    t_validate = validate_claims()
    t_enrich = enrich_claims()
    t_score = score_claims()
    t_publish = publish_to_kafka()
    t_store = store_rejections()
    t_reconcile = reconcile_run()
    t_summary = update_summary()
    t_archive = archive_input()
    t_notify = notify()

    t_check >> t_validate >> t_enrich >> t_score >> t_publish

    # Rejections are filed independently of the publish outcome.
    t_score >> t_store

    # Reconciliation waits for both sides of the split.
    [t_publish, t_store] >> t_reconcile

    # Archiving is gated on reconciliation, never on the publish task alone.
    t_reconcile >> [t_summary, t_archive] >> t_notify
