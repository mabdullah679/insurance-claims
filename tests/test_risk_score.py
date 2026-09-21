"""Risk scoring: the properties that hold regardless of the real formula.

The client has not supplied the scoring rules, so there is nothing here that
asserts a specific score for a specific claim -- that would be asserting the
placeholder is correct, which it is not. What IS tested is every property the
pipeline depends on and that any replacement scorer must also satisfy:
bounded, integer, deterministic, and total (never rejects).

`TestQaWorkbookExpectations` documents the gap rather than hiding it.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from claims.score import risk
from claims.score.risk import (FixedScorer, LinearBandScorer, TableScorer,
                               SCORE_MAX, SCORE_MIN)

# The scores the QA workbook expects, which no known formula reproduces.
QA_EXPECTED = {
    "CLM-QA-20260911-001": 18,
    "CLM-QA-20260911-002": 35,
    "CLM-QA-20260911-003": 12,
    "CLM-QA-20260911-004": 52,
    "CLM-QA-20260911-005": 24,
}


def enriched(claim_id="C1", amount="1000.00", coverage=50000, tenure=5.0):
    return {"claim_id": claim_id, "customer_id": "CUS1", "policy_id": "POL1",
            "claim_amount": Decimal(amount), "coverage_limit": coverage,
            "customer_tenure_years": tenure}


class TestContract:
    """Properties every scorer must satisfy, placeholder or real."""

    def test_score_is_an_int(self):
        assert isinstance(LinearBandScorer().score(enriched()), int)

    @pytest.mark.parametrize("amount,coverage,tenure", [
        ("0.01", 1_000_000, 50.0),
        ("999999999.00", 1, 0.0),
        ("1000.00", 0, 5.0),
        ("1000.00", None, None),
    ])
    def test_score_is_always_within_bounds(self, amount, coverage, tenure):
        s = LinearBandScorer().score(
            enriched(amount=amount, coverage=coverage, tenure=tenure))
        assert SCORE_MIN <= s <= SCORE_MAX

    def test_scoring_is_deterministic(self):
        """A corrected re-run must publish the same score for the same claim."""
        scorer = LinearBandScorer()
        claim = enriched()
        assert len({scorer.score(claim) for _ in range(20)}) == 1

    def test_missing_optional_fields_do_not_crash(self):
        assert isinstance(LinearBandScorer().score({"claim_id": "C1"}), int)

    def test_scoring_never_rejects(self):
        """Scoring is total: every enriched claim gets a score."""
        result = risk.run([enriched(f"C{i}") for i in range(5)])
        assert result.ok_count == 5
        assert result.rejected_count == 0

    def test_every_record_gains_a_risk_score(self):
        result = risk.run([enriched("C1"), enriched("C2")])
        assert all("risk_score" in r for r in result.records)

    def test_original_fields_are_preserved(self):
        result = risk.run([enriched("C1")])
        assert result.records[0]["claim_id"] == "C1"
        assert result.records[0]["claim_amount"] == Decimal("1000.00")


class TestOutOfContractScorerIsRejected:
    """A bad scorer must fail loudly rather than publish a nonsense event."""

    def test_out_of_range_score_raises(self):
        class Bad:
            name = "bad"
            def score(self, claim):
                return 500

        with pytest.raises(ValueError, match="expected an int"):
            risk.run([enriched()], scorer=Bad())

    def test_non_integer_score_raises(self):
        class Bad:
            name = "bad"
            def score(self, claim):
                return 42.7

        with pytest.raises(ValueError, match="expected an int"):
            risk.run([enriched()], scorer=Bad())


class TestPlaceholderDirection:
    """The placeholder's shape is defensible even though its weights are invented."""

    def test_larger_claims_score_higher_all_else_equal(self):
        s = LinearBandScorer()
        assert s.score(enriched(amount="500.00")) < s.score(enriched(amount="20000.00"))

    def test_higher_exposure_scores_higher(self):
        s = LinearBandScorer()
        low = s.score(enriched(amount="1000.00", coverage=1_000_000))
        high = s.score(enriched(amount="1000.00", coverage=5_000))
        assert low < high

    def test_longer_tenure_scores_lower(self):
        s = LinearBandScorer()
        assert s.score(enriched(tenure=20.0)) < s.score(enriched(tenure=0.0))

    def test_is_flagged_as_a_placeholder_in_metrics(self):
        """The run metrics must make it obvious these are not real scores."""
        assert risk.run([enriched()]).metrics["is_placeholder"] is True

    def test_a_real_scorer_is_not_flagged_as_placeholder(self):
        assert risk.run([enriched()], scorer=FixedScorer(1)).metrics["is_placeholder"] is False


class TestPluggability:
    """Swapping in the client's model must touch nothing but the scorer."""

    def test_custom_scorer_is_used(self):
        result = risk.run([enriched()], scorer=FixedScorer(77))
        assert result.records[0]["risk_score"] == 77

    def test_scorer_name_is_recorded_for_traceability(self):
        result = risk.run([enriched()], scorer=FixedScorer(1))
        assert result.metrics["scorer"] == "fixed"

    def test_table_scorer_reproduces_known_values(self):
        """How the QA fixture can be driven to its documented output today."""
        scorer = TableScorer(QA_EXPECTED)
        claims = [enriched(cid) for cid in QA_EXPECTED]
        result = risk.run(claims, scorer=scorer)
        assert [r["risk_score"] for r in result.records] == list(QA_EXPECTED.values())

    def test_table_scorer_falls_back_for_unknown_claims(self):
        scorer = TableScorer(QA_EXPECTED, fallback=FixedScorer(9))
        assert scorer.score(enriched("UNKNOWN-CLAIM")) == 9


class TestQaWorkbookExpectations:
    """Documents the open question rather than pretending it is answered."""

    def test_placeholder_does_not_reproduce_the_qa_scores(self):
        """EXPECTED TO FAIL THE QA DIFF until the client supplies the rules.

        This test asserts the gap exists. When the real scorer arrives, set
        CLAIMS_RISK_SCORER and this test should be replaced by one that
        asserts the QA workbook's values are reproduced exactly.
        """
        scorer = LinearBandScorer()
        produced = {cid: scorer.score(enriched(cid)) for cid in QA_EXPECTED}
        assert produced != QA_EXPECTED, (
            "the placeholder now matches the QA workbook -- if the real "
            "scoring rules have arrived, replace this test with a positive one")

    def test_qa_scores_are_not_monotonic_in_exposure_ratio(self):
        """Evidence that the formula is not simply amount / coverage_limit."""
        ratios = {
            "CLM-QA-20260911-001": 1250.75 / 50000,
            "CLM-QA-20260911-002": 4899.00 / 350000,
            "CLM-QA-20260911-003": 725.40 / 75000,
            "CLM-QA-20260911-004": 9875.25 / 500000,
            "CLM-QA-20260911-005": 2150.00 / 50000,
        }
        by_ratio = [c for c, _ in sorted(ratios.items(), key=lambda kv: kv[1])]
        by_score = [c for c, _ in sorted(QA_EXPECTED.items(), key=lambda kv: kv[1])]
        assert by_ratio != by_score
