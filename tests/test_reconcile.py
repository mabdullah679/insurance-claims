"""Reconciliation: the arithmetic that gates the archive.

Every test here is really about one question -- can this run's input file be
safely moved out of the inbound bucket? -- because that is the irreversible
step reconciliation protects.
"""
from __future__ import annotations

import pytest

from claims.common.outcomes import ReconciliationError
from claims.common.reconcile import (RunTotals, reconcile,
                                     rejection_rate_breached)


def totals(**over) -> RunTotals:
    base = dict(input_records=5, validation_rejected=0, enrichment_rejected=0,
                successful=5, kafka_attempted=5, kafka_acknowledged=5)
    return RunTotals(**{**base, **over})


class TestHappyPath:
    def test_clean_run_reconciles(self):
        summary = reconcile(totals())
        assert summary["input_records"] == 5
        assert summary["successful"] == 5

    def test_run_with_rejections_reconciles(self):
        t = totals(successful=3, validation_rejected=1, enrichment_rejected=1,
                   kafka_attempted=3, kafka_acknowledged=3)
        summary = reconcile(t)
        assert summary["rejected"] == 2
        assert summary["rejection_rate"] == 0.4

    def test_all_rejected_is_a_valid_reconciled_run(self):
        """Every record bad is still an accounted-for run, not a failure."""
        t = totals(successful=0, validation_rejected=5, kafka_attempted=0,
                   kafka_acknowledged=0)
        assert reconcile(t)["successful"] == 0

    def test_empty_but_consistent_run(self):
        t = RunTotals()
        assert reconcile(t)["input_records"] == 0


class TestLostRecords:
    def test_missing_records_raise(self):
        """3 + 1 != 5: one record vanished somewhere in the pipeline."""
        t = totals(successful=3, validation_rejected=1, kafka_attempted=3,
                   kafka_acknowledged=3)
        with pytest.raises(ReconciliationError, match="unaccounted"):
            reconcile(t)

    def test_error_names_how_many_are_missing(self):
        t = totals(successful=2, validation_rejected=1, kafka_attempted=2,
                   kafka_acknowledged=2)
        with pytest.raises(ReconciliationError, match="2 record"):
            reconcile(t)

    def test_more_output_than_input_also_raises(self):
        """Duplication is as much a reconciliation failure as loss."""
        t = totals(input_records=3, successful=5, kafka_acknowledged=5)
        with pytest.raises(ReconciliationError, match="unaccounted"):
            reconcile(t)


class TestKafkaAcknowledgement:
    def test_ack_shortfall_raises(self):
        """The requirement is an equality, and this is where it is enforced."""
        t = totals(kafka_acknowledged=4)
        with pytest.raises(ReconciliationError, match="acknowledged"):
            reconcile(t)

    def test_ack_excess_raises(self):
        t = totals(kafka_acknowledged=6)
        with pytest.raises(ReconciliationError, match="acknowledged"):
            reconcile(t)

    def test_zero_acks_with_successful_claims_raises(self):
        t = totals(kafka_acknowledged=0)
        with pytest.raises(ReconciliationError):
            reconcile(t)


class TestStoredRejections:
    def test_stored_count_must_match_rejected_count(self):
        t = totals(successful=4, validation_rejected=1, kafka_attempted=4,
                   kafka_acknowledged=4)
        with pytest.raises(ReconciliationError, match="stored"):
            reconcile(t, rejections_stored=0)

    def test_matching_stored_count_passes(self):
        t = totals(successful=4, validation_rejected=1, kafka_attempted=4,
                   kafka_acknowledged=4)
        assert reconcile(t, rejections_stored=1)["rejected"] == 1

    def test_unknown_stored_count_is_not_checked(self):
        """None means the store task did not report; do not invent a failure."""
        t = totals(successful=4, validation_rejected=1, kafka_attempted=4,
                   kafka_acknowledged=4)
        assert reconcile(t, rejections_stored=None)


class TestMultipleViolations:
    def test_every_problem_is_reported_at_once(self):
        """One fix per run of the DAG is a slow way to repair a pipeline."""
        t = totals(successful=3, validation_rejected=0, kafka_acknowledged=1)
        with pytest.raises(ReconciliationError) as e:
            reconcile(t)
        msg = str(e.value)
        assert "unaccounted" in msg and "acknowledged" in msg


class TestRejectionRateAlert:
    """Section 5: an alert condition, explicitly NOT a failure condition."""

    def test_breach_is_detected(self):
        t = totals(input_records=10, successful=8, validation_rejected=2,
                   kafka_attempted=8, kafka_acknowledged=8)
        assert rejection_rate_breached(t, 0.10) is True

    def test_at_threshold_is_not_a_breach(self):
        t = totals(input_records=10, successful=9, validation_rejected=1,
                   kafka_attempted=9, kafka_acknowledged=9)
        assert rejection_rate_breached(t, 0.10) is False

    def test_breach_does_not_stop_reconciliation(self):
        """A high rejection rate still reconciles: the records are accounted for."""
        t = totals(input_records=10, successful=1, validation_rejected=9,
                   kafka_attempted=1, kafka_acknowledged=1)
        assert reconcile(t)["rejection_rate"] == 0.9
        assert rejection_rate_breached(t, 0.10) is True

    def test_empty_run_never_breaches(self):
        assert rejection_rate_breached(RunTotals(), 0.10) is False
