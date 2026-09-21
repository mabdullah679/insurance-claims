"""Risk scoring -- THE ONE THING THE SPECIFICATION DOES NOT DEFINE.

STATUS: BLOCKED ON THE CLIENT. Read this before trusting any score.

Requirement 9 says "calculate a risk score for every claim" and the Kafka
field table calls it a "calculated fraud or operational risk value". Neither
the requirements PDF nor the QA workbook states HOW.

The QA workbook does pin exact expected outputs:

    CLM-QA-20260911-001   1,250.75   coverage 50,000    AUTO      -> 18
    CLM-QA-20260911-002   4,899.00   coverage 350,000   HOME      -> 35
    CLM-QA-20260911-003     725.40   coverage 75,000    AUTO      -> 12
    CLM-QA-20260911-004   9,875.25   coverage 500,000   HOME      -> 52
    CLM-QA-20260911-005   2,150.00   coverage 50,000    RENTERS   -> 24

Those five scores are monotonic in claim amount, which is suggestive and not
sufficient. They are NOT monotonic in amount/coverage ratio (2.5%, 1.4%, 1.0%,
2.0%, 4.3% against scores 18, 35, 12, 52, 24), score/amount is not constant,
and five points cannot determine a formula over the eight-or-so available
inputs (amount, coverage limit, policy type, customer age, tenure, state,
policy dates). Any formula fitted to them would be numerology.

So this module does NOT guess. `LinearBandScorer` is a DOCUMENTED PLACEHOLDER
that produces plausible, bounded, deterministic scores so the rest of the
pipeline can be built, tested and demonstrated end to end. It will NOT
reproduce the QA workbook's numbers, and the QA comparison step will show a
risk_score mismatch until the real rules arrive.

TO PLUG IN THE REAL MODEL: implement `RiskScorer` and pass it to
`claims.score.risk.run(...)`, or set CLAIMS_RISK_SCORER to a
"module:attribute" path. Nothing else in the pipeline changes.

QUESTIONS FOR THE CLIENT (see docs/OPEN-QUESTIONS.md):
  1. What are the scoring inputs and their weights?
  2. What is the output range -- 0-100 integer, as the QA data implies?
  3. Is it a deterministic rule set, or a model artifact we must load?
  4. Must a given claim always score identically on a re-run of the same
     logical date? (Assumed yes; see IDEMPOTENCE below.)

IDEMPOTENCE: the requirements permit a corrected re-run for the same logical
date, and the reconciliation compares Kafka counts against claim counts. A
scorer that used wall-clock time or randomness would make a re-run publish
different values for the same claim. Every scorer here must be a pure function
of the claim record.
"""
from __future__ import annotations

import importlib
import logging
import os
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from claims.common.outcomes import StageResult

log = logging.getLogger(__name__)

SCORE_MIN = 0
SCORE_MAX = 100


@runtime_checkable
class RiskScorer(Protocol):
    """A pure function from an enriched claim to a bounded integer score.

    Implementations must be deterministic: the same enriched claim must always
    produce the same score, on any host, on any day.
    """

    name: str

    def score(self, claim: dict[str, Any]) -> int:
        ...


class LinearBandScorer:
    """PLACEHOLDER. Plausible, deterministic, and not the client's model.

    The shape is a defensible first approximation of an operational risk
    score, built only from fields the QA fixtures actually supply:

        exposure   how large the claim is against the policy's coverage limit
        magnitude  how large the claim is in absolute terms
        tenure     newer customers carry more unknown risk than long-tenured

    It is bounded to 0-100 and rounded to an integer to match the shape of the
    QA data. The WEIGHTS ARE INVENTED. Do not present the numbers this
    produces as risk assessments to David Chen.
    """

    name = "placeholder-linear-band-v0"

    # Named so a reader can see at a glance that these are arbitrary and
    # meant to be replaced wholesale, not tuned.
    W_EXPOSURE = 55.0
    W_MAGNITUDE = 30.0
    W_TENURE = 15.0

    # A claim at or above this fraction of the coverage limit saturates the
    # exposure term.
    EXPOSURE_SATURATION = 0.25
    # Likewise for absolute size, in currency units.
    MAGNITUDE_SATURATION = 25_000.0
    # Tenure at or beyond this many years contributes no additional risk.
    TENURE_SATURATION_YEARS = 10.0

    def score(self, claim: dict[str, Any]) -> int:
        amount = _as_float(claim.get("claim_amount"))
        coverage = _as_float(claim.get("coverage_limit"))
        tenure_years = _as_float(claim.get("customer_tenure_years"))

        # Exposure: undefined without a coverage limit, so contribute the
        # midpoint rather than 0 -- an unknown is not the same as "safe".
        if coverage and coverage > 0:
            exposure = min(amount / coverage, self.EXPOSURE_SATURATION) / self.EXPOSURE_SATURATION
        else:
            exposure = 0.5

        magnitude = min(amount, self.MAGNITUDE_SATURATION) / self.MAGNITUDE_SATURATION

        # Tenure reduces risk, so the RISK contribution is its inverse.
        t = min(max(tenure_years, 0.0), self.TENURE_SATURATION_YEARS)
        tenure_risk = 1.0 - (t / self.TENURE_SATURATION_YEARS)

        raw = (self.W_EXPOSURE * exposure
               + self.W_MAGNITUDE * magnitude
               + self.W_TENURE * tenure_risk)
        return int(max(SCORE_MIN, min(SCORE_MAX, round(raw))))


class FixedScorer:
    """Returns one constant. For tests that care about plumbing, not scoring."""

    name = "fixed"

    def __init__(self, value: int = 0):
        self.value = int(value)

    def score(self, claim: dict[str, Any]) -> int:
        return self.value


class TableScorer:
    """Replays known scores by claim ID, falling back to a delegate.

    This exists so the QA fixture can be driven to its documented expected
    output while the real formula is still unknown -- it makes the rest of the
    pipeline verifiable against `Expected_Kafka` today, and it is transparently
    a lookup table rather than a model.

    NEVER configure this in production: it would silently score unknown claims
    by the fallback while appearing to be a scoring implementation.
    """

    name = "table"

    def __init__(self, table: dict[str, int], fallback: RiskScorer | None = None):
        self.table = dict(table)
        self.fallback = fallback or LinearBandScorer()

    def score(self, claim: dict[str, Any]) -> int:
        cid = claim.get("claim_id")
        if cid in self.table:
            return int(self.table[cid])
        return self.fallback.score(claim)


def default_scorer() -> RiskScorer:
    """The configured scorer, or the documented placeholder.

    CLAIMS_RISK_SCORER takes "package.module:attribute", where the attribute is
    either a RiskScorer instance or a zero-argument callable returning one.
    """
    spec = os.environ.get("CLAIMS_RISK_SCORER", "").strip()
    if not spec:
        log.warning(
            "risk scoring is using %s -- a PLACEHOLDER, not the client's model. "
            "Scores will not match the QA workbook. See claims/score/risk.py.",
            LinearBandScorer.name)
        return LinearBandScorer()

    if ":" not in spec:
        raise ValueError(
            f"CLAIMS_RISK_SCORER must be 'module:attribute', got {spec!r}")
    mod_name, attr = spec.split(":", 1)
    obj = getattr(importlib.import_module(mod_name), attr)
    scorer = obj() if callable(obj) and not isinstance(obj, RiskScorer) else obj
    if not isinstance(scorer, RiskScorer):
        raise TypeError(f"{spec} is not a RiskScorer (needs .name and .score)")
    log.info("risk scoring using %s", getattr(scorer, "name", spec))
    return scorer


def _as_float(v: Any) -> float:
    if v is None or v == "":
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def run(claims: list[dict[str, Any]], scorer: RiskScorer | None = None) -> StageResult:
    """Score every claim. Scoring never rejects.

    A claim that reaches this stage has already been validated and enriched;
    if the scorer cannot compute a meaningful score from it the right answer
    is a bounded default, not a rejection, because rejecting here would mean
    a claim disappears from the published stream for a reason the business
    never asked us to reject on.
    """
    scorer = scorer or default_scorer()
    scored: list[dict[str, Any]] = []
    for claim in claims:
        value = scorer.score(claim)
        if not isinstance(value, int) or not (SCORE_MIN <= value <= SCORE_MAX):
            raise ValueError(
                f"scorer {getattr(scorer, 'name', scorer)!r} returned {value!r} "
                f"for claim {claim.get('claim_id')!r}; expected an int in "
                f"[{SCORE_MIN}, {SCORE_MAX}]")
        scored.append({**claim, "risk_score": value})

    result = StageResult(records=scored)
    result.metrics = {
        "scorer": getattr(scorer, "name", "unknown"),
        "scored": len(scored),
        "is_placeholder": isinstance(scorer, LinearBandScorer),
    }
    return result
