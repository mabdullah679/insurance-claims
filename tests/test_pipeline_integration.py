"""End-to-end runs through every stage, with fakes at the boundaries.

These tests are the ones that would catch a regression in how the STAGES FIT
TOGETHER -- the unit tests each prove one module keeps its own promise, and
this file proves the promises compose into the outcomes the requirements
describe.

The scenarios are taken directly from the requirements' own vocabulary: a
happy path, a record-rejection path that still succeeds, and the two failure
paths that must not.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from claims.common.outcomes import (DependencyError, InputFileError,
                                    PublishError, ReconciliationError)
from claims.common.reconcile import reconcile
from claims.common.staging import Staging
from claims.enrich import join
from claims.publish import kafka_publish as kp
from claims.score import risk
from claims.score.risk import TableScorer
from claims.validate import records as v
from tests.conftest import (QA_CSV, QA_CUSTOMERS, QA_DATE, QA_POLICIES,
                            FakeLookup, FakeProducer)
from tests.test_risk_score import QA_EXPECTED


def run_pipeline(csv_text, customers, policies, producer, *, tmp_path,
                 scorer=None, sleep=lambda s: None, cfg=None):
    """The whole chain, as the DAG wires it, against a temp staging dir."""
    from claims.common.config import KafkaConfig

    cfg = cfg or KafkaConfig(bootstrap="unused:9092",
                             topic="insurance.claims.processed",
                             schema_version="1.0", max_retries=3,
                             retry_delay_seconds=0.0, batch_size=500)
    staging = Staging(tmp_path, QA_DATE)

    rows = v.parse_csv(csv_text)
    validated = v.validate(rows, expected_date=QA_DATE)
    staging.write_stage("validated", validated.records)
    staging.write_rejections("validation", validated.rejections)

    enriched = join.run(validated.records, customers, policies, as_of=QA_DATE)
    staging.write_stage("enriched", enriched.records)
    staging.write_rejections("enrichment", enriched.rejections)

    scored = risk.run(enriched.records, scorer=scorer)
    staging.write_stage("scored", scored.records)

    metrics = kp.run(scored.records, dag_run_id="manual__2026-09-11_qa",
                     cfg=cfg, producer=producer, sleep=sleep)
    staging.write_metrics("kafka", metrics)
    return staging


class TestHappyPath:
    """The QA workbook's declared expectation: SUCCESS, 0 rejections, 5 acks."""

    def test_five_claims_published_and_reconciled(self, tmp_path, qa_customers,
                                                  qa_policies):
        fake = FakeProducer()
        staging = run_pipeline(QA_CSV, qa_customers, qa_policies, fake,
                               tmp_path=tmp_path)
        totals = staging.totals()

        assert totals.input_records == 5
        assert totals.successful == 5
        assert totals.rejected == 0
        assert totals.kafka_acknowledged == 5
        reconcile(totals)                               # must not raise

    def test_published_messages_match_the_expected_kafka_tab(
            self, tmp_path, qa_customers, qa_policies):
        """Every field of Expected_Kafka except the two that cannot match.

        risk_score is driven by TableScorer here because the real formula is
        unknown; processing_timestamp is a wall-clock value the workbook
        leaves as the placeholder QA_RUN_TIME.
        """
        fake = FakeProducer()
        run_pipeline(QA_CSV, qa_customers, qa_policies, fake,
                     tmp_path=tmp_path, scorer=TableScorer(QA_EXPECTED))

        msgs = {m["claim_id"]: m for m in fake.messages("insurance.claims.processed")}
        assert len(msgs) == 5

        first = msgs["CLM-QA-20260911-001"]
        assert first["customer_id"] == "CUS-QA-1001"
        assert first["policy_id"] == "POL-QA-5001"
        assert first["claim_date"] == "2026-09-11"
        assert first["claim_amount"] == "1250.75"
        assert first["risk_score"] == 18
        assert first["processing_status"] == "SUCCESS"
        assert first["dag_run_id"] == "manual__2026-09-11_qa"
        assert first["schema_version"] == "1.0"

    def test_all_five_expected_risk_scores_reproduce(self, tmp_path,
                                                     qa_customers, qa_policies):
        fake = FakeProducer()
        run_pipeline(QA_CSV, qa_customers, qa_policies, fake,
                     tmp_path=tmp_path, scorer=TableScorer(QA_EXPECTED))
        got = {m["claim_id"]: m["risk_score"]
               for m in fake.messages("insurance.claims.processed")}
        assert got == QA_EXPECTED


class TestRejectionsStillSucceed:
    """"A properly stored rejected claim does not automatically fail the DAG.\""""

    def test_bad_records_are_rejected_and_the_run_reconciles(
            self, tmp_path, qa_customers, qa_policies):
        csv = QA_CSV + ("CLM-QA-20260911-006,CUS-QA-1006,POL-QA-5006,bad,2026-09-11\n"
                        "CLM-QA-20260911-001,CUS-QA-1001,POL-QA-5001,50.00,2026-09-11\n")
        fake = FakeProducer()
        staging = run_pipeline(csv, qa_customers, qa_policies, fake,
                               tmp_path=tmp_path)
        totals = staging.totals()

        assert totals.input_records == 7
        assert totals.successful == 5
        assert totals.rejected == 2
        assert totals.kafka_acknowledged == 5
        reconcile(totals, rejections_stored=2)          # a SUCCESSFUL run

    def test_every_rejected_record_carries_a_reason(self, tmp_path,
                                                    qa_customers, qa_policies):
        csv = QA_CSV + "CLM-QA-20260911-006,CUS-QA-1006,POL-QA-5006,bad,2026-09-11\n"
        staging = run_pipeline(csv, qa_customers, qa_policies, FakeProducer(),
                               tmp_path=tmp_path)
        rows = staging.read_all_rejections()
        assert rows and all(r.get("rejection_reason") for r in rows)
        assert all(r.get("rejection_detail") for r in rows)

    def test_enrichment_rejections_are_counted_separately(self, tmp_path,
                                                          qa_customers):
        policies = FakeLookup(QA_POLICIES, missing={"POL-QA-5003"})
        staging = run_pipeline(QA_CSV, qa_customers, policies, FakeProducer(),
                               tmp_path=tmp_path)
        totals = staging.totals()
        assert totals.enrichment_rejected == 1
        assert totals.validation_rejected == 0
        assert totals.successful == 4
        reconcile(totals, rejections_stored=1)

    def test_high_rejection_rate_still_reconciles(self, tmp_path, qa_customers):
        """An alert-worthy day is still an accounted-for day."""
        policies = FakeLookup(QA_POLICIES, missing=set(QA_POLICIES) - {"POL-QA-5001"})
        staging = run_pipeline(QA_CSV, qa_customers, policies, FakeProducer(),
                               tmp_path=tmp_path)
        totals = staging.totals()
        assert totals.rejection_rate == 0.8
        reconcile(totals, rejections_stored=4)


class TestPlatformFailures:
    """These must fail the DAG, and must NOT reach the archive step."""

    def test_empty_input_fails_before_anything_runs(self, tmp_path):
        with pytest.raises(InputFileError):
            v.parse_csv("")

    def test_dependency_outage_fails_the_run(self, tmp_path, qa_policies):
        broken = FakeLookup(QA_CUSTOMERS, fail=True)
        with pytest.raises(DependencyError):
            run_pipeline(QA_CSV, broken, qa_policies, FakeProducer(),
                         tmp_path=tmp_path)

    def test_kafka_exhaustion_fails_the_run(self, tmp_path, qa_customers,
                                            qa_policies):
        fake = FakeProducer(fail_claims={"CLM-QA-20260911-003"})
        with pytest.raises(PublishError):
            run_pipeline(QA_CSV, qa_customers, qa_policies, fake,
                         tmp_path=tmp_path)

    def test_kafka_failure_leaves_the_run_unreconciled(self, tmp_path,
                                                       qa_customers, qa_policies):
        """THE ARCHIVE SAFETY PROPERTY.

        A publish failure must leave the counts un-reconcilable, because
        `archive_input_file` runs only downstream of `reconcile_run`. If this
        ever passes reconciliation, a broker outage would archive the only
        re-runnable copy of the day's claims.
        """
        fake = FakeProducer(fail_claims={"CLM-QA-20260911-003"})
        with pytest.raises(PublishError):
            run_pipeline(QA_CSV, qa_customers, qa_policies, fake,
                         tmp_path=tmp_path)

        # The kafka metrics were never written, so the totals cannot reconcile.
        staging = Staging(tmp_path, QA_DATE)
        totals = staging.totals()
        assert totals.kafka_acknowledged == 0
        assert totals.successful == 5
        with pytest.raises(ReconciliationError, match="acknowledged"):
            reconcile(totals)


class TestIdempotentRerun:
    """"Permit a corrected run to be executed for the same logical date.\""""

    def test_rerunning_produces_identical_messages(self, tmp_path, qa_customers,
                                                   qa_policies):
        from datetime import datetime, timezone
        ts = datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc)
        scored = risk.run(
            join.run(v.validate(v.parse_csv(QA_CSV)).records,
                     qa_customers, qa_policies, as_of=QA_DATE).records,
            scorer=TableScorer(QA_EXPECTED)).records

        a = [kp.build_message(c, dag_run_id="r", schema_version="1.0",
                              processed_at=ts) for c in scored]
        b = [kp.build_message(c, dag_run_id="r", schema_version="1.0",
                              processed_at=ts) for c in scored]
        assert a == b

    def test_rerun_overwrites_staging_rather_than_appending(
            self, tmp_path, qa_customers, qa_policies):
        for _ in range(3):
            staging = run_pipeline(QA_CSV, qa_customers, qa_policies,
                                   FakeProducer(), tmp_path=tmp_path)
        assert staging.totals().input_records == 5


class TestDecimalSurvivesStaging:
    def test_amounts_round_trip_as_decimal_between_tasks(self, tmp_path,
                                                         qa_customers, qa_policies):
        """Float drift in a financial reconciliation is not acceptable."""
        staging = run_pipeline(QA_CSV, qa_customers, qa_policies,
                               FakeProducer(), tmp_path=tmp_path)
        records = staging.read_stage("scored")
        assert all(isinstance(r["claim_amount"], Decimal) for r in records)
        assert any(r["claim_amount"] == Decimal("1250.75") for r in records)

    def test_dates_round_trip_as_date(self, tmp_path, qa_customers, qa_policies):
        staging = run_pipeline(QA_CSV, qa_customers, qa_policies,
                               FakeProducer(), tmp_path=tmp_path)
        records = staging.read_stage("scored")
        assert all(isinstance(r["claim_date"], date) for r in records)
