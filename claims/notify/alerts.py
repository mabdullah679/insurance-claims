"""Notifications -- requirements section 5.

The notification table names four audiences and what each needs to see. That
routing is the design: the point is not "send an email", it is that James
Wilson gets an actionable failure with a run ID while Michael Brown gets to
know whether he can trust the claim status a customer is asking about.

`Notifier` is a protocol with a logging default, so the DAG is complete and
runnable before anyone decides whether this shop uses Slack, PagerDuty, SNS or
email -- and so tests can assert on what WOULD have been sent.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from claims.common.config import AlertConfig
from claims.common.reconcile import RunTotals, rejection_rate_breached

log = logging.getLogger(__name__)


@dataclass
class Notification:
    """One message, addressed and titled. Rendering is the notifier's job."""

    channel: str
    subject: str
    body: str
    severity: str = "INFO"
    fields: dict[str, Any] | None = None


class Notifier(Protocol):
    def send(self, note: Notification) -> None:
        ...


class LoggingNotifier:
    """Default sink: writes to the task log, and records what it sent.

    Deliberately not a no-op. A silently-dropped alert is worse than no alert
    system at all, so even the default leaves evidence in the Airflow UI.
    """

    def __init__(self) -> None:
        self.sent: list[Notification] = []

    def send(self, note: Notification) -> None:
        self.sent.append(note)
        log.log(logging.ERROR if note.severity in ("ERROR", "CRITICAL") else logging.INFO,
                "[%s] to %s: %s\n%s", note.severity, note.channel,
                note.subject, note.body)


def success(processing_date: date, totals: RunTotals, *, dag_run_id: str,
            duration_seconds: float | None = None,
            cfg: AlertConfig | None = None) -> Notification:
    """The completion notification -- requirement 15.

    Carries exactly what the requirements' notification table asks for: run
    date, total, successful, rejected, Kafka acknowledged, duration.
    """
    cfg = cfg or AlertConfig()
    lines = [
        f"Daily claims processing completed for {processing_date}.",
        "",
        f"  Input records        {totals.input_records}",
        f"  Successful           {totals.successful}",
        f"  Rejected             {totals.rejected}",
        f"  Kafka acknowledged   {totals.kafka_acknowledged}",
    ]
    if totals.kafka_retries:
        lines.append(f"  Kafka retries        {totals.kafka_retries}")
    if duration_seconds is not None:
        lines.append(f"  Duration             {duration_seconds:.0f}s")
    lines += ["", f"DAG run: {dag_run_id}"]
    return Notification(channel=cfg.success_channel,
                        subject=f"Claims processing succeeded — {processing_date}",
                        body="\n".join(lines), severity="INFO",
                        fields=totals.as_dict())


def rejections(processing_date: date, totals: RunTotals, *, error_location: str,
               cfg: AlertConfig | None = None) -> Notification | None:
    """Record-rejection notification, to Claims Ops and Data Quality.

    Returns None when nothing was rejected: a clean run should not generate a
    "0 rejections" message every morning, because an alert people learn to
    ignore is not an alert. Severity escalates when the rate breaches the
    configured threshold.
    """
    cfg = cfg or AlertConfig()
    if not totals.rejected:
        return None

    breached = rejection_rate_breached(totals, cfg.rejection_rate_threshold)
    top = sorted(totals.rejection_reasons.items(), key=lambda kv: -kv[1])[:5]
    lines = [
        f"{totals.rejected} of {totals.input_records} claim(s) were rejected "
        f"for {processing_date} ({totals.rejection_rate:.1%}).",
        "",
        f"Error location: {error_location}",
        "",
        "Top rejection reasons:",
    ]
    lines += [f"  {count:>5}  {reason}" for reason, count in top]
    if breached:
        lines += ["",
                  f"*** Rejection rate {totals.rejection_rate:.1%} exceeds the "
                  f"{cfg.rejection_rate_threshold:.0%} threshold. ***"]
    return Notification(channel=cfg.rejection_channel,
                        subject=(f"{'ALERT: high ' if breached else ''}"
                                 f"Claim rejections — {processing_date} "
                                 f"({totals.rejected})"),
                        body="\n".join(lines),
                        severity="WARNING" if breached else "INFO",
                        fields=totals.as_dict())


def failure(processing_date: date, *, failed_task: str, error: str,
            dag_run_id: str, try_number: int | None = None,
            max_tries: int | None = None,
            cfg: AlertConfig | None = None) -> Notification:
    """DAG-failure alert, to Data Engineering and Claims Operations.

    Carries all five fields the requirements' notification table asks for:
    failed task, error summary, run ID, retry status, and operational impact.
    The impact is stated in words rather than left for the reader to infer
    from a stack trace: downstream consumers are not receiving claims, which
    is what makes it urgent rather than merely broken.
    """
    cfg = cfg or AlertConfig()
    lines = [
        f"Daily claims processing FAILED for {processing_date}.",
        "",
        f"  Failed task   {failed_task}",
        f"  DAG run       {dag_run_id}",
    ]
    if try_number is not None:
        retry_status = (f"attempt {try_number} of {max_tries + 1}"
                        if max_tries is not None else f"attempt {try_number}")
        exhausted = max_tries is not None and try_number >= max_tries + 1
        lines.append(f"  Retry status  {retry_status}"
                     f"{' - retries exhausted' if exhausted else ''}")
    lines += [
        "",
        "Error:",
        f"  {error}",
        "",
        "Operational impact: processed claims have NOT been published to "
        "Kafka for this date. Downstream claim review and fraud scoring are "
        "blocked, and customer-facing claim status is stale. The input file "
        "has not been archived, so this logical date can be re-run once the "
        "cause is fixed.",
    ]
    return Notification(channel=cfg.failure_channel,
                        subject=f"FAILED: Claims processing — {processing_date}",
                        body="\n".join(lines), severity="ERROR")


def recovery(processing_date: date, totals: RunTotals, *, dag_run_id: str,
             cfg: AlertConfig | None = None) -> Notification:
    """Recovery notification, which also reaches Customer Service.

    Michael Brown is on this list because the requirements put him there: he
    fielded the calls during the outage and needs to know the status his
    customers see is current again.
    """
    cfg = cfg or AlertConfig()
    channel = f"{cfg.failure_channel},{cfg.success_channel},customer-service"
    body = "\n".join([
        f"Claims processing recovered for {processing_date}.",
        "",
        f"  Recovered run     {dag_run_id}",
        f"  Restored date     {processing_date}",
        f"  Claims published  {totals.kafka_acknowledged}",
        f"  Rejected          {totals.rejected}",
        "",
        "Backlog is cleared for this date; downstream claim status is current.",
    ])
    return Notification(channel=channel,
                        subject=f"Recovered: Claims processing — {processing_date}",
                        body=body, severity="INFO", fields=totals.as_dict())
