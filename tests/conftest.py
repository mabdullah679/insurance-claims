"""Shared fixtures and fakes.

Every fake here honours the same contract the real gateway does -- most
importantly, a lookup RAISES DependencyError when it cannot answer and returns
a possibly-empty dict when it can. Tests that blur that line would pass while
the production code silently converted outages into rejections, which is the
exact bug the design is built to prevent.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from claims.common.outcomes import DependencyError

# The QA workbook's five claims, as the CSV that lands in S3.
QA_CSV = """claim_id,customer_id,policy_id,claim_amount,claim_date
CLM-QA-20260911-001,CUS-QA-1001,POL-QA-5001,1250.75,2026-09-11
CLM-QA-20260911-002,CUS-QA-1002,POL-QA-5002,4899.00,2026-09-11
CLM-QA-20260911-003,CUS-QA-1003,POL-QA-5003,725.40,2026-09-11
CLM-QA-20260911-004,CUS-QA-1004,POL-QA-5004,9875.25,2026-09-11
CLM-QA-20260911-005,CUS-QA-1005,POL-QA-5005,2150.00,2026-09-11
"""

QA_DATE = date(2026, 9, 11)

# Dates are the workbook's Excel serials decoded (1899-12-30 epoch), so this
# fake answers exactly what the client's Customers_QA tab says.
QA_CUSTOMERS = {
    "CUS-QA-1001": {"customer_id": "CUS-QA-1001", "customer_name": "Olivia Martin",
                    "date_of_birth": "1988-04-14", "state": "GA",
                    "customer_since": "2018-06-05", "customer_status": "ACTIVE"},
    "CUS-QA-1002": {"customer_id": "CUS-QA-1002", "customer_name": "Ethan Walker",
                    "date_of_birth": "1979-11-23", "state": "GA",
                    "customer_since": "2015-02-12", "customer_status": "ACTIVE"},
    "CUS-QA-1003": {"customer_id": "CUS-QA-1003", "customer_name": "Sophia Nguyen",
                    "date_of_birth": "1993-08-02", "state": "FL",
                    "customer_since": "2021-09-19", "customer_status": "ACTIVE"},
    "CUS-QA-1004": {"customer_id": "CUS-QA-1004", "customer_name": "Noah Thompson",
                    "date_of_birth": "1985-01-30", "state": "TX",
                    "customer_since": "2012-12-01", "customer_status": "ACTIVE"},
    "CUS-QA-1005": {"customer_id": "CUS-QA-1005", "customer_name": "Ava Robinson",
                    "date_of_birth": "1990-06-17", "state": "NC",
                    "customer_since": "2020-03-27", "customer_status": "ACTIVE"},
}

QA_POLICIES = {
    "POL-QA-5001": {"policy_id": "POL-QA-5001", "customer_id": "CUS-QA-1001",
                    "policy_type": "AUTO", "coverage_limit": 50000,
                    "effective_date": "2026-01-01", "expiration_date": "2026-12-31",
                    "policy_status": "ACTIVE"},
    "POL-QA-5002": {"policy_id": "POL-QA-5002", "customer_id": "CUS-QA-1002",
                    "policy_type": "HOME", "coverage_limit": 350000,
                    "effective_date": "2026-03-01", "expiration_date": "2027-02-28",
                    "policy_status": "ACTIVE"},
    "POL-QA-5003": {"policy_id": "POL-QA-5003", "customer_id": "CUS-QA-1003",
                    "policy_type": "AUTO", "coverage_limit": 75000,
                    "effective_date": "2026-05-15", "expiration_date": "2027-05-14",
                    "policy_status": "ACTIVE"},
    "POL-QA-5004": {"policy_id": "POL-QA-5004", "customer_id": "CUS-QA-1004",
                    "policy_type": "HOME", "coverage_limit": 500000,
                    "effective_date": "2026-02-10", "expiration_date": "2027-02-09",
                    "policy_status": "ACTIVE"},
    "POL-QA-5005": {"policy_id": "POL-QA-5005", "customer_id": "CUS-QA-1005",
                    "policy_type": "RENTERS", "coverage_limit": 50000,
                    "effective_date": "2026-07-01", "expiration_date": "2027-06-30",
                    "policy_status": "ACTIVE"},
}


class FakeLookup:
    """A lookup that answers from a dict. Honours the real contract."""

    def __init__(self, rows: dict, *, fail: bool = False, missing: set | None = None):
        self.rows = rows
        self.fail = fail
        self.missing = missing or set()
        self.calls: list[list[str]] = []

    def fetch(self, ids: list[str]) -> dict:
        self.calls.append(list(ids))
        if self.fail:
            # This is the outage case: no answer at all, never an empty dict.
            raise DependencyError("simulated dependency outage")
        return {k: v for k, v in self.rows.items()
                if k in set(ids) - self.missing}


class FakeFuture:
    def __init__(self, error: Exception | None = None):
        self.error = error

    def get(self, timeout=None):
        if self.error:
            raise self.error
        return {"partition": 0, "offset": 1}


class FakeProducer:
    """Records sends and can be told to fail specific claims.

    `fail_claims` fails those claim IDs on every attempt; `fail_times` fails
    everything for the first N attempts and then succeeds, which is how the
    retry-then-recover path is exercised deterministically.
    """

    def __init__(self, fail_claims: set | None = None, fail_times: int = 0):
        self.sent: dict[str, list] = {}
        self.fail_claims = fail_claims or set()
        self.fail_times = fail_times
        self.attempt = 0
        self.flushes = 0

    def send(self, topic, key=None, value=None):
        self.sent.setdefault(topic, []).append((key, value))
        claim_id = value.get("claim_id") if isinstance(value, dict) else None
        if claim_id in self.fail_claims:
            return FakeFuture(RuntimeError("broker rejected this message"))
        if self.fail_times and self.attempt < self.fail_times:
            return FakeFuture(RuntimeError("broker unavailable"))
        return FakeFuture()

    def flush(self, timeout=None):
        self.flushes += 1
        self.attempt += 1

    def close(self, timeout=None):
        pass

    def messages(self, topic):
        return [v for _, v in self.sent.get(topic, [])]


@pytest.fixture
def qa_customers():
    return FakeLookup(QA_CUSTOMERS)


@pytest.fixture
def qa_policies():
    return FakeLookup(QA_POLICIES)


@pytest.fixture
def no_sleep():
    """Collapse retry delays so the retry tests do not actually wait."""
    calls = []
    yield lambda s: calls.append(s)
