"""Enrichment from PostgreSQL and the policy API -- requirements 6, 7, 8.

THE DISTINCTION THIS STAGE EXISTS TO PROTECT
--------------------------------------------
    "customer CUS-QA-1001 does not exist"     -> reject that one claim
    "PostgreSQL refused the connection"       -> fail the DAG

Both arrive as an absent customer record if you are careless, and conflating
them is a silent-data-loss bug: a five-minute database blip would reject every
claim in the run, write them all to the error folder as if the DATA were bad,
report SUCCESS, and archive the input file. Jessica Patel would spend a morning
investigating perfectly good claims.

So the lookup protocols are contracted to RAISE `DependencyError` when they
cannot answer, and to return a dict-of-found-records when they can. Absence in
a successful answer is a rejection; an exception is a failure. A gateway that
cannot distinguish the two must raise.

Lookups are BATCHED rather than per-claim (one IN query, paged API calls) --
at a daily file's volume, per-row round trips are the difference between a
minute and an hour, and the requirements set an operational completion window.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Protocol

from claims.common.outcomes import (DependencyError, Rejection,
                                    RejectionReason, StageResult)

log = logging.getLogger(__name__)


class CustomerLookup(Protocol):
    """Batch customer fetch from PostgreSQL.

    MUST raise DependencyError if the database cannot be reached or the query
    fails. MUST NOT return a partial result to signal an error -- a missing
    key means "this customer does not exist", and nothing else.
    """

    def fetch(self, customer_ids: list[str]) -> dict[str, dict[str, Any]]:
        ...


class PolicyLookup(Protocol):
    """Batch policy fetch from the REST API. Same contract as CustomerLookup."""

    def fetch(self, policy_ids: list[str]) -> dict[str, dict[str, Any]]:
        ...


# Policy states we will publish against. Anything else is a rejection with
# POLICY_NOT_ACTIVE -- the QA fixtures are all ACTIVE, and the requirements'
# QA guide expects "all policy lookups return ACTIVE".
ACTIVE_POLICY_STATES = frozenset({"ACTIVE"})


def run(claims: list[dict[str, Any]],
        customers: CustomerLookup,
        policies: PolicyLookup,
        *,
        as_of: date | None = None,
        require_active_policy: bool = True) -> StageResult:
    """Join each claim to its customer and policy. Requirement 8.

    `as_of` enables the policy-period check: a claim dated outside its policy's
    effective window is rejected rather than published, because a paid claim
    against an expired policy is exactly the kind of thing the downstream fraud
    review exists to catch. When `as_of` is None the check is skipped and only
    policy STATUS is considered.
    """
    if not claims:
        return StageResult(metrics={"customers_looked_up": 0, "policies_looked_up": 0})

    customer_ids = sorted({c["customer_id"] for c in claims if c.get("customer_id")})
    policy_ids = sorted({c["policy_id"] for c in claims if c.get("policy_id")})

    # A failure inside either lookup propagates: these are DependencyErrors by
    # contract, and catching them here is precisely the bug described above.
    customer_rows = customers.fetch(customer_ids)
    policy_rows = policies.fetch(policy_ids)

    _assert_lookup_shape(customer_rows, "customer")
    _assert_lookup_shape(policy_rows, "policy")

    ok: list[dict[str, Any]] = []
    rejections: list[Rejection] = []

    for claim in claims:
        cid = claim.get("customer_id")
        pid = claim.get("policy_id")

        customer = customer_rows.get(cid)
        if customer is None:
            rejections.append(Rejection(
                reason=RejectionReason.CUSTOMER_NOT_FOUND,
                detail=f"no customer record for {cid}",
                record=claim, claim_id=claim.get("claim_id")))
            continue

        policy = policy_rows.get(pid)
        if policy is None:
            rejections.append(Rejection(
                reason=RejectionReason.POLICY_NOT_FOUND,
                detail=f"no policy record for {pid}",
                record=claim, claim_id=claim.get("claim_id")))
            continue

        if require_active_policy:
            problem = _policy_problem(policy, claim.get("claim_date"), as_of)
            if problem:
                rejections.append(Rejection(
                    reason=RejectionReason.POLICY_NOT_ACTIVE,
                    detail=f"policy {pid}: {problem}",
                    record=claim, claim_id=claim.get("claim_id")))
                continue

        ok.append(_merge(claim, customer, policy))

    result = StageResult(records=ok, rejections=rejections)
    result.metrics = {
        "customers_looked_up": len(customer_ids),
        "customers_found": len(customer_rows),
        "policies_looked_up": len(policy_ids),
        "policies_found": len(policy_rows),
    }
    return result


def _assert_lookup_shape(rows: Any, what: str) -> None:
    """A lookup that returns None has confused 'no answer' with 'no match'."""
    if rows is None:
        raise DependencyError(
            f"{what} lookup returned None; it must return a dict (possibly "
            f"empty) on success and raise DependencyError on failure")
    if not isinstance(rows, dict):
        raise DependencyError(
            f"{what} lookup returned {type(rows).__name__}, expected dict")


def _policy_problem(policy: dict[str, Any], claim_date: Any, as_of: date | None) -> str | None:
    """Why this policy cannot back a claim, or None if it can."""
    status = str(policy.get("policy_status", "")).strip().upper()
    if status not in ACTIVE_POLICY_STATES:
        return f"status is {status or 'UNKNOWN'}"

    check_date = claim_date if isinstance(claim_date, date) else as_of
    if check_date is None:
        return None

    effective = _as_date(policy.get("effective_date"))
    expiration = _as_date(policy.get("expiration_date"))
    if effective and check_date < effective:
        return f"claim dated {check_date} precedes effective date {effective}"
    if expiration and check_date > expiration:
        return f"claim dated {check_date} follows expiration date {expiration}"
    return None


def _merge(claim: dict[str, Any], customer: dict[str, Any],
           policy: dict[str, Any]) -> dict[str, Any]:
    """One enriched claim.

    Source fields are namespaced onto the claim rather than blindly merged, so
    a `state` column in the claims export cannot overwrite the customer's
    state, and a reader can tell where every value came from.
    """
    enriched = dict(claim)
    enriched.update({
        "customer_name": customer.get("customer_name"),
        "customer_state": customer.get("state"),
        "customer_status": customer.get("customer_status"),
        "customer_since": customer.get("customer_since"),
        "customer_tenure_years": _tenure_years(customer.get("customer_since"),
                                               claim.get("claim_date")),
        "policy_type": policy.get("policy_type"),
        "coverage_limit": policy.get("coverage_limit"),
        "policy_status": policy.get("policy_status"),
        "policy_effective_date": policy.get("effective_date"),
        "policy_expiration_date": policy.get("expiration_date"),
    })
    return enriched


def _tenure_years(since: Any, claim_date: Any) -> float:
    """Customer tenure in years -- an input the placeholder scorer uses."""
    start = _as_date(since)
    end = claim_date if isinstance(claim_date, date) else _as_date(claim_date)
    if not start or not end or end < start:
        return 0.0
    return round((end - start).days / 365.25, 3)


def _as_date(v: Any) -> date | None:
    if isinstance(v, date):
        return v
    if v is None or v == "":
        return None
    try:
        from datetime import datetime
        return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
