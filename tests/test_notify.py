"""Notifications -- requirements section 5.

The notification table names four audiences and the MINIMUM CONTENT each one
needs. These tests assert that content is actually present, because a
notification that reaches the right person without the fact they need is a
notification that gets escalated by phone.

`claims/notify/alerts.py` had no test file at all before this one, and two
defects lived here: the failure alert's "retry status" field was never
populated, and the recovery notification was built but never sent by anything.
"""
from __future__ import annotations

from datetime import date

import pytest

from claims.common.config import AlertConfig
from claims.common.reconcile import RunTotals
from claims.notify import alerts

DAY = date(2026, 9, 11)


def totals(**over) -> RunTotals:
    base = dict(input_records=5, successful=5, validation_rejected=0,
                enrichment_rejected=0, kafka_attempted=5, kafka_acknowledged=5)
    return RunTotals(**{**base, **over})


class TestSuccessNotification:
    """Row 1: Claims Operations needs run date, totals, acks, duration."""

    def test_carries_every_field_the_table_requires(self):
        note = alerts.success(DAY, totals(), dag_run_id="run-1",
                              duration_seconds=412)
        body = note.body
        assert "2026-09-11" in body
        assert "5" in body
        assert "Kafka acknowledged" in body
        assert "412s" in body
        assert "run-1" in body

    def test_goes_to_claims_operations(self):
        note = alerts.success(DAY, totals(), dag_run_id="r")
        assert note.channel == AlertConfig().success_channel
        assert note.severity == "INFO"

    def test_reports_retries_when_there_were_any(self):
        note = alerts.success(DAY, totals(kafka_retries=2), dag_run_id="r")
        assert "Kafka retries" in note.body

    def test_stays_quiet_about_retries_on_a_clean_run(self):
        assert "Kafka retries" not in alerts.success(
            DAY, totals(), dag_run_id="r").body


class TestRejectionNotification:
    """Row 2: Claims Ops and Data Quality need count, location, top reasons."""

    def test_carries_count_location_and_reasons(self):
        t = totals(successful=3, validation_rejected=2, kafka_attempted=3,
                   kafka_acknowledged=3,
                   rejection_reasons={"DUPLICATE_CLAIM_ID": 1,
                                      "INVALID_CLAIM_AMOUNT": 1})
        note = alerts.rejections(DAY, t, error_location="s3://b/errors/dt=20260911/")
        assert "2 of 5" in note.body
        assert "s3://b/errors/dt=20260911/" in note.body
        assert "DUPLICATE_CLAIM_ID" in note.body

    def test_reaches_both_audiences(self):
        t = totals(successful=4, validation_rejected=1, kafka_attempted=4,
                   kafka_acknowledged=4)
        note = alerts.rejections(DAY, t, error_location="s3://x")
        assert "data-quality" in note.channel

    def test_a_clean_run_sends_nothing(self):
        """An alert people learn to ignore is not an alert."""
        assert alerts.rejections(DAY, totals(), error_location="n/a") is None

    def test_breaching_the_threshold_escalates_severity(self):
        """Section 5: rejected-record rate above 10 percent."""
        t = totals(input_records=10, successful=5, validation_rejected=5,
                   kafka_attempted=5, kafka_acknowledged=5)
        note = alerts.rejections(DAY, t, error_location="s3://x")
        assert note.severity == "WARNING"
        assert "exceeds" in note.body
        assert note.subject.startswith("ALERT")

    def test_below_the_threshold_stays_informational(self):
        t = totals(input_records=100, successful=99, validation_rejected=1,
                   kafka_attempted=99, kafka_acknowledged=99)
        note = alerts.rejections(DAY, t, error_location="s3://x")
        assert note.severity == "INFO"


class TestFailureNotification:
    """Row 3: failed task, error summary, run ID, retry status, impact.

    The retry-status line is the one that was missing: `try_number` was an
    accepted parameter that the DAG never passed.
    """

    def test_carries_all_five_required_fields(self):
        note = alerts.failure(DAY, failed_task="publish_to_kafka",
                              error="broker unreachable after 3 retries",
                              dag_run_id="scheduled__2026-09-12",
                              try_number=3, max_tries=2)
        body = note.body
        assert "publish_to_kafka" in body                    # failed task
        assert "broker unreachable" in body                  # error summary
        assert "scheduled__2026-09-12" in body               # run ID
        assert "Retry status" in body                        # retry status
        assert "Operational impact" in body                  # impact

    def test_retry_status_names_the_attempt_and_the_budget(self):
        note = alerts.failure(DAY, failed_task="t", error="e",
                              dag_run_id="r", try_number=2, max_tries=2)
        assert "attempt 2 of 3" in note.body

    def test_exhausted_retries_are_called_out(self):
        """The operator's next action differs when nothing will retry."""
        note = alerts.failure(DAY, failed_task="t", error="e",
                              dag_run_id="r", try_number=3, max_tries=2)
        assert "retries exhausted" in note.body

    def test_a_retry_still_pending_is_not_called_exhausted(self):
        note = alerts.failure(DAY, failed_task="t", error="e",
                              dag_run_id="r", try_number=1, max_tries=2)
        assert "retries exhausted" not in note.body

    def test_retry_status_is_omitted_when_unknown(self):
        note = alerts.failure(DAY, failed_task="t", error="e", dag_run_id="r")
        assert "Retry status" not in note.body

    def test_reaches_data_engineering_and_claims_operations(self):
        note = alerts.failure(DAY, failed_task="t", error="e", dag_run_id="r")
        assert "data-engineering" in note.channel
        assert "claims-operations" in note.channel
        assert note.severity == "ERROR"

    def test_says_the_input_file_was_not_archived(self):
        """The re-run instruction depends on this being stated."""
        note = alerts.failure(DAY, failed_task="t", error="e", dag_run_id="r")
        assert "not been archived" in note.body


class TestRecoveryNotification:
    """Row 4: also reaches Customer Service."""

    def test_carries_run_id_restored_date_and_backlog_status(self):
        note = alerts.recovery(DAY, totals(), dag_run_id="manual__repair")
        assert "manual__repair" in note.body
        assert "2026-09-11" in note.body
        assert "Backlog" in note.body

    def test_reaches_customer_service(self):
        """Michael Brown fielded the calls and needs to know status is current."""
        note = alerts.recovery(DAY, totals(), dag_run_id="r")
        assert "customer-service" in note.channel

    def test_reports_how_many_claims_were_published(self):
        note = alerts.recovery(DAY, totals(), dag_run_id="r")
        assert "Claims published  5" in note.body


class TestLoggingNotifier:
    def test_records_what_it_sent(self):
        """A silently dropped alert is worse than no alert system."""
        notifier = alerts.LoggingNotifier()
        notifier.send(alerts.success(DAY, totals(), dag_run_id="r"))
        assert len(notifier.sent) == 1

    def test_errors_are_logged_at_error_level(self, caplog):
        notifier = alerts.LoggingNotifier()
        with caplog.at_level("ERROR"):
            notifier.send(alerts.failure(DAY, failed_task="t", error="e",
                                         dag_run_id="r"))
        assert any(r.levelname == "ERROR" for r in caplog.records)
