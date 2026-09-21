# `claims/score/` — risk scoring

**Requirement 9.** Attaches an integer `risk_score` (0-100) to every enriched
claim.

> ## ⚠️ The scorer shipped here is a PLACEHOLDER
>
> It is **not** the client's model and **will not** reproduce the QA
> workbook's expected values. This is the one open item that blocks QA
> sign-off.

## Why it is a placeholder

Neither the requirements PDF nor the QA workbook says how a risk score is
calculated. The workbook does pin five exact expected outputs:

| Claim | Amount | Coverage | Type | Expected |
|---|---:|---:|---|---:|
| ...001 | 1,250.75 | 50,000 | AUTO | 18 |
| ...002 | 4,899.00 | 350,000 | HOME | 35 |
| ...003 | 725.40 | 75,000 | AUTO | 12 |
| ...004 | 9,875.25 | 500,000 | HOME | 52 |
| ...005 | 2,150.00 | 50,000 | RENTERS | 24 |

Those are monotonic in claim amount, which is suggestive and not sufficient.
They are **not** monotonic in amount ÷ coverage (2.5%, 1.4%, 1.0%, 2.0%, 4.3%
against 18, 35, 12, 52, 24), and five points cannot determine a formula over
the eight available inputs. Any formula fitted to them would be numerology, so
we did not guess. See `docs/OPEN-QUESTIONS.md` Q1.

## Plugging in the real scorer

Implement the protocol and point one environment variable at it. Nothing else
in the pipeline changes.

```python
class ProductionScorer:
    name = "production-v1"
    def score(self, claim: dict) -> int: ...     # 0-100, deterministic
```

```bash
export CLAIMS_RISK_SCORER="your_package.scoring:ProductionScorer"
```

## The contract every scorer must satisfy

| Property | Why |
|---|---|
| Returns an `int` in 0-100 | Out-of-range raises rather than publishing a nonsense event |
| **Deterministic** | A corrected re-run of the same date must republish identical scores, or Kafka reconciliation is meaningless |
| **Total** — never rejects | A claim reaching this stage is already valid and enriched; a bounded default beats dropping it for a reason the business never asked for |

Wall-clock time and randomness are therefore forbidden inside a scorer.

## Scorers in the box

- `LinearBandScorer` — the placeholder. Invented weights, clearly labelled.
- `FixedScorer(n)` — returns a constant, for plumbing tests.
- `TableScorer(mapping)` — replays known scores by claim ID. **Never configure
  this in production**: it would silently score unknown claims by its fallback
  while appearing to be a real implementation. It exists so the QA fixture can
  be driven to its documented output while the formula is unknown.

## Observability

Run metrics carry `is_placeholder: true` while the placeholder is active, and
the task logs a warning on every run. When the real scorer lands, replace
`tests/test_risk_score.py::test_placeholder_does_not_reproduce_the_qa_scores`
with a positive assertion.
