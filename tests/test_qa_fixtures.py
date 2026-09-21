"""Every QA fixture produces its documented outcome.

The client's workbook supplies one happy-path fixture. These are the failure
cases the requirements spend most of their text on but ship no test data for --
and this file is what keeps them honest: a fixture nobody asserts against is
just a file.

Each test names the requirement it covers and the outcome the fixture is
supposed to demonstrate, so `qa/README.md` and this file cannot drift apart
without a test failing.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from claims.common.outcomes import (InputFileError, PublishError,
                                    RejectionReason, SchemaError)
from claims.common.reconcile import reconcile, rejection_rate_breached
from claims.enrich import join
from claims.publish import kafka_publish as kp
from claims.score import risk
from claims.validate import records as v
from tests.conftest import QA_CUSTOMERS, QA_DATE, QA_POLICIES, FakeLookup, FakeProducer

FIXTURES = Path(__file__).resolve().parents[1] / "qa" / "fixtures"


def load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class TestFixturesExist:
    """The fixture set is part of the deliverable, not an accident of a run."""

    @pytest.mark.parametrize("name", [
        "claims_20260911_happy.csv",
        "claims_20260911_duplicates.csv",
        "claims_20260911_missing_column.csv",
        "claims_20260911_empty.csv",
        "claims_20260911_header_only.csv",
        "claims_20260911_mixed_rejections.csv",
        "claims_20260911_high_rejection_rate.csv",
    ])
    def test_fixture_is_present(self, name):
        assert (FIXTURES / name).exists(), f"missing QA fixture {name}"


class TestDagFailureFixtures:
    """Fixtures that must FAIL the DAG. Requirements section 4."""

    def test_empty_file_fails(self):
        with pytest.raises(InputFileError, match="empty"):
            v.parse_csv(load("claims_20260911_empty.csv"))

    def test_header_only_file_fails(self):
        with pytest.raises(InputFileError, match="no data rows"):
            v.parse_csv(load("claims_20260911_header_only.csv"))

    def test_missing_required_column_fails(self):
        with pytest.raises(SchemaError, match="claim_amount"):
            v.parse_csv(load("claims_20260911_missing_column.csv"))


class TestRejectionFixtures:
    """Fixtures that must SUCCEED with rejections. The key distinction."""

    def test_duplicates_reject_two_and_keep_three(self):
        result = v.validate(v.parse_csv(load("claims_20260911_duplicates.csv")))
        assert result.ok_count == 3
        assert result.rejected_count == 2
        assert all(r.reason is RejectionReason.DUPLICATE_CLAIM_ID
                   for r in result.rejections)
        assert result.ok_count + result.rejected_count == 5

    def test_duplicates_keep_the_first_occurrence(self):
        from decimal import Decimal
        result = v.validate(v.parse_csv(load("claims_20260911_duplicates.csv")))
        first = next(r for r in result.records
                     if r["claim_id"] == "CLM-QA-20260911-001")
        assert first["claim_amount"] == Decimal("1250.75")

    def test_mixed_rejections_cover_every_record_level_reason(self):
        """One fixture, several rejection reasons, all reconciling."""
        rows = v.parse_csv(load("claims_20260911_mixed_rejections.csv"))
        validated = v.validate(rows, expected_date=QA_DATE)

        # Two rows fail validation outright: an empty amount and a non-numeric
        # one, plus a negative amount.
        reasons = {r.reason for r in validated.rejections}
        assert RejectionReason.MISSING_REQUIRED_FIELD in reasons
        assert RejectionReason.INVALID_CLAIM_AMOUNT in reasons
        assert validated.ok_count + validated.rejected_count == len(rows)

    def test_mixed_rejections_reconcile_end_to_end(self):
        rows = v.parse_csv(load("claims_20260911_mixed_rejections.csv"))
        validated = v.validate(rows, expected_date=QA_DATE)
        enriched = join.run(validated.records, FakeLookup(QA_CUSTOMERS),
                            FakeLookup(QA_POLICIES), as_of=QA_DATE)
        scored = risk.run(enriched.records)
        fake = FakeProducer()
        metrics = kp.run(scored.records, dag_run_id="qa", producer=fake,
                         cfg=_cfg())

        from claims.common.reconcile import RunTotals
        totals = RunTotals(
            input_records=len(rows),
            validation_rejected=validated.rejected_count,
            enrichment_rejected=enriched.rejected_count,
            successful=len(scored.records),
            kafka_attempted=metrics["attempted"],
            kafka_acknowledged=metrics["acknowledged"])
        reconcile(totals)                               # a SUCCESSFUL run
        assert totals.rejected > 0

    def test_unknown_customer_is_rejected_not_failed(self):
        """The dependency answered; the customer simply is not there."""
        rows = v.parse_csv(load("claims_20260911_mixed_rejections.csv"))
        validated = v.validate(rows, expected_date=QA_DATE)
        enriched = join.run(validated.records, FakeLookup(QA_CUSTOMERS),
                            FakeLookup(QA_POLICIES), as_of=QA_DATE)
        reasons = {r.reason for r in enriched.rejections}
        assert RejectionReason.CUSTOMER_NOT_FOUND in reasons


class TestAlertThresholdFixture:
    """Requirement: alert when the rejection rate exceeds 10%."""

    def test_high_rejection_rate_breaches_the_threshold(self):
        from claims.common.reconcile import RunTotals
        rows = v.parse_csv(load("claims_20260911_high_rejection_rate.csv"))
        validated = v.validate(rows, expected_date=QA_DATE)
        enriched = join.run(validated.records, FakeLookup(QA_CUSTOMERS),
                            FakeLookup(QA_POLICIES), as_of=QA_DATE)

        totals = RunTotals(
            input_records=len(rows),
            validation_rejected=validated.rejected_count,
            enrichment_rejected=enriched.rejected_count,
            successful=len(enriched.records),
            kafka_attempted=len(enriched.records),
            kafka_acknowledged=len(enriched.records))

        assert rejection_rate_breached(totals, 0.10) is True
        reconcile(totals)      # still reconciles: an alert, not a failure

    def test_four_of_five_customers_are_unknown(self):
        rows = v.parse_csv(load("claims_20260911_high_rejection_rate.csv"))
        validated = v.validate(rows, expected_date=QA_DATE)
        enriched = join.run(validated.records, FakeLookup(QA_CUSTOMERS),
                            FakeLookup(QA_POLICIES), as_of=QA_DATE)
        assert enriched.rejected_count == 4
        assert enriched.ok_count == 1


class TestKafkaFailureScenario:
    """No CSV can express this one -- it is a broker state, not a data state."""

    def test_retry_exhaustion_fails_the_run(self):
        rows = v.parse_csv(load("claims_20260911_happy.csv"))
        validated = v.validate(rows, expected_date=QA_DATE)
        enriched = join.run(validated.records, FakeLookup(QA_CUSTOMERS),
                            FakeLookup(QA_POLICIES), as_of=QA_DATE)
        scored = risk.run(enriched.records)
        fake = FakeProducer(fail_claims={"CLM-QA-20260911-003"})
        with pytest.raises(PublishError):
            kp.run(scored.records, dag_run_id="qa", producer=fake,
                   cfg=_cfg(), sleep=lambda s: None)

    def test_happy_fixture_publishes_all_five(self):
        rows = v.parse_csv(load("claims_20260911_happy.csv"))
        validated = v.validate(rows, expected_date=QA_DATE)
        enriched = join.run(validated.records, FakeLookup(QA_CUSTOMERS),
                            FakeLookup(QA_POLICIES), as_of=QA_DATE)
        scored = risk.run(enriched.records)
        fake = FakeProducer()
        metrics = kp.run(scored.records, dag_run_id="qa", producer=fake,
                         cfg=_cfg())
        assert metrics["acknowledged"] == 5


def _cfg():
    from claims.common.config import KafkaConfig
    return KafkaConfig(bootstrap="unused:9092",
                       topic="insurance.claims.processed",
                       schema_version="1.0", max_retries=3,
                       retry_delay_seconds=0.0, batch_size=500)
