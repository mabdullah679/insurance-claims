"""Enrichment: the outage-versus-absence distinction.

This file exists mostly for one test class, `TestOutageIsNotRejection`. If
those tests ever start passing while the production code rejects claims on a
dependency failure, the pipeline has acquired a silent-data-loss bug that
looks like a successful run.
"""
from __future__ import annotations

import pytest

from claims.common.outcomes import DependencyError, RejectionReason
from claims.enrich import join
from claims.validate import records as v
from tests.conftest import (QA_CSV, QA_CUSTOMERS, QA_DATE, QA_POLICIES,
                            FakeLookup)


def qa_claims():
    return v.validate(v.parse_csv(QA_CSV), expected_date=QA_DATE).records


class TestHappyPath:
    def test_all_five_enrich(self, qa_customers, qa_policies):
        result = join.run(qa_claims(), qa_customers, qa_policies, as_of=QA_DATE)
        assert result.ok_count == 5
        assert result.rejected_count == 0

    def test_customer_and_policy_fields_are_joined_on(self, qa_customers, qa_policies):
        result = join.run(qa_claims(), qa_customers, qa_policies, as_of=QA_DATE)
        first = result.records[0]
        assert first["customer_name"] == "Olivia Martin"
        assert first["policy_type"] == "AUTO"
        assert first["coverage_limit"] == 50000

    def test_lookups_are_batched_not_per_claim(self, qa_customers, qa_policies):
        """Five claims must not mean five round trips to each dependency."""
        join.run(qa_claims(), qa_customers, qa_policies, as_of=QA_DATE)
        assert len(qa_customers.calls) == 1
        assert len(qa_policies.calls) == 1
        assert len(qa_customers.calls[0]) == 5

    def test_tenure_is_derived_for_scoring(self, qa_customers, qa_policies):
        result = join.run(qa_claims(), qa_customers, qa_policies, as_of=QA_DATE)
        assert all(r["customer_tenure_years"] > 0 for r in result.records)


class TestOutageIsNotRejection:
    """A dependency that cannot answer must FAIL THE DAG.

    The failure mode being prevented: a database blip rejects every claim,
    they are written to the error folder as if the data were bad, the run
    reports SUCCESS, and the input file is archived.
    """

    def test_postgres_outage_raises(self, qa_policies):
        broken = FakeLookup(QA_CUSTOMERS, fail=True)
        with pytest.raises(DependencyError):
            join.run(qa_claims(), broken, qa_policies, as_of=QA_DATE)

    def test_policy_api_outage_raises(self, qa_customers):
        broken = FakeLookup(QA_POLICIES, fail=True)
        with pytest.raises(DependencyError):
            join.run(qa_claims(), qa_customers, broken, as_of=QA_DATE)

    def test_outage_rejects_nothing_at_all(self, qa_policies):
        """Not "rejects fewer" -- rejects NOTHING. It never gets that far."""
        broken = FakeLookup(QA_CUSTOMERS, fail=True)
        with pytest.raises(DependencyError):
            join.run(qa_claims(), broken, qa_policies, as_of=QA_DATE)

    def test_lookup_returning_none_is_treated_as_a_failure(self, qa_policies):
        """A gateway that returns None has confused 'no answer' with 'no match'."""
        class NoneLookup:
            def fetch(self, ids):
                return None

        with pytest.raises(DependencyError, match="None"):
            join.run(qa_claims(), NoneLookup(), qa_policies, as_of=QA_DATE)


class TestAbsenceIsRejection:
    """A dependency that answers correctly, with no match, rejects one claim."""

    def test_missing_customer_rejects_only_that_claim(self, qa_policies):
        customers = FakeLookup(QA_CUSTOMERS, missing={"CUS-QA-1003"})
        result = join.run(qa_claims(), customers, qa_policies, as_of=QA_DATE)
        assert result.ok_count == 4
        assert result.rejected_count == 1
        assert result.rejections[0].reason is RejectionReason.CUSTOMER_NOT_FOUND
        assert result.rejections[0].claim_id == "CLM-QA-20260911-003"

    def test_missing_policy_rejects_only_that_claim(self, qa_customers):
        policies = FakeLookup(QA_POLICIES, missing={"POL-QA-5002"})
        result = join.run(qa_claims(), qa_customers, policies, as_of=QA_DATE)
        assert result.ok_count == 4
        assert result.rejections[0].reason is RejectionReason.POLICY_NOT_FOUND

    def test_inactive_policy_is_rejected(self, qa_customers):
        policies = dict(QA_POLICIES)
        policies["POL-QA-5001"] = {**policies["POL-QA-5001"],
                                   "policy_status": "LAPSED"}
        result = join.run(qa_claims(), qa_customers, FakeLookup(policies),
                          as_of=QA_DATE)
        assert result.rejected_count == 1
        assert result.rejections[0].reason is RejectionReason.POLICY_NOT_ACTIVE
        assert "LAPSED" in result.rejections[0].detail

    def test_claim_after_policy_expiry_is_rejected(self, qa_customers):
        policies = dict(QA_POLICIES)
        policies["POL-QA-5001"] = {**policies["POL-QA-5001"],
                                   "expiration_date": "2026-08-31"}
        result = join.run(qa_claims(), qa_customers, FakeLookup(policies),
                          as_of=QA_DATE)
        assert result.rejections[0].reason is RejectionReason.POLICY_NOT_ACTIVE
        assert "expiration" in result.rejections[0].detail

    def test_claim_before_policy_effective_date_is_rejected(self, qa_customers):
        policies = dict(QA_POLICIES)
        policies["POL-QA-5001"] = {**policies["POL-QA-5001"],
                                   "effective_date": "2026-09-30"}
        result = join.run(qa_claims(), qa_customers, FakeLookup(policies),
                          as_of=QA_DATE)
        assert result.rejections[0].reason is RejectionReason.POLICY_NOT_ACTIVE
        assert "effective" in result.rejections[0].detail


class TestReconciliationHolds:
    def test_counts_always_add_up(self, qa_customers):
        policies = FakeLookup(QA_POLICIES, missing={"POL-QA-5002", "POL-QA-5004"})
        claims = qa_claims()
        result = join.run(claims, qa_customers, policies, as_of=QA_DATE)
        assert result.ok_count + result.rejected_count == len(claims)

    def test_empty_input_is_not_an_error(self, qa_customers, qa_policies):
        result = join.run([], qa_customers, qa_policies, as_of=QA_DATE)
        assert result.ok_count == 0 and result.rejected_count == 0
