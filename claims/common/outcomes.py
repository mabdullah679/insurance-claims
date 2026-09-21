"""The two kinds of "bad" in this pipeline, kept apart in the type system.

The requirements draw one distinction and then build everything else on it
(requirements PDF section 4, "Important distinction"):

    A REJECTED RECORD is a handled business outcome. A claim missing a
    customer, or carrying a malformed amount, is data the business expects to
    see. It is routed to the S3 error folder with a reason and the DAG still
    SUCCEEDS.

    A DAG FAILURE is platform work that could not complete: the input file is
    missing, PostgreSQL is unreachable, the policy API is down, or Kafka
    publishing exhausted its retries.

Conflating the two is the most expensive mistake available here, in both
directions. Failing the DAG on a rejected record pages James Wilson at 2am for
a customer who typed a letter into an amount field. Swallowing a Kafka outage
as a "rejection" silently drops real claims -- Sarah Mitchell never sees them,
and nothing alerts.

So rejection is a VALUE (`Rejection`, carried in a result) and failure is an
EXCEPTION (`ClaimsPipelineError`, which propagates and fails the task). A
stage cannot accidentally express one as the other.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RejectionReason(str, Enum):
    """Why a single claim record was not publishable.

    Stored with the rejected record in the S3 error folder, and aggregated
    into the "top rejection reasons" line of the rejection notification, so
    Jessica Patel can see the shape of a bad day without opening the files.

    A str-Enum so it serializes into JSON as its value with no encoder.
    """

    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_CLAIM_AMOUNT = "INVALID_CLAIM_AMOUNT"
    INVALID_CLAIM_DATE = "INVALID_CLAIM_DATE"
    DUPLICATE_CLAIM_ID = "DUPLICATE_CLAIM_ID"
    CUSTOMER_NOT_FOUND = "CUSTOMER_NOT_FOUND"
    POLICY_NOT_FOUND = "POLICY_NOT_FOUND"
    POLICY_NOT_ACTIVE = "POLICY_NOT_ACTIVE"
    SCHEMA_VALIDATION_FAILED = "SCHEMA_VALIDATION_FAILED"


@dataclass(frozen=True)
class Rejection:
    """One claim that will not be published, and why.

    `claim_id` may be None: a row can be rejected precisely BECAUSE it has no
    claim ID, and that row still has to be written to the error folder rather
    than vanishing. Reconciliation counts rows, not IDs, for this reason.
    """

    reason: RejectionReason
    detail: str
    record: dict[str, Any]
    claim_id: str | None = None

    def to_error_row(self) -> dict[str, Any]:
        """Flatten for the S3 error folder: original columns plus the verdict."""
        return {
            **self.record,
            "rejection_reason": self.reason.value,
            "rejection_detail": self.detail,
        }


# ── Failures: these fail the DAG ──────────────────────────────────────────

class ClaimsPipelineError(Exception):
    """Base for every condition that must fail the DAG.

    Raised, never returned. If you find yourself catching one of these and
    turning it into a Rejection, stop -- that converts an outage into silent
    data loss.
    """


class InputFileError(ClaimsPipelineError):
    """The expected S3 object is missing, empty, or unreadable.

    Requirements 1-2: the file must exist and must not be empty. Emily Carter
    either did not upload, or uploaded something broken; either way there is
    no run to be had.
    """


class SchemaError(ClaimsPipelineError):
    """The file is present but does not carry the required columns.

    This is FILE-level, not record-level, and so is a failure rather than a
    rejection: if `claim_amount` is missing as a COLUMN, every row is
    unprocessable and rejecting all of them would produce a "successful" run
    with zero published claims and a 100% rejection rate.
    """


class DependencyError(ClaimsPipelineError):
    """PostgreSQL or the policy API could not be reached or answered badly.

    Distinct from CUSTOMER_NOT_FOUND / POLICY_NOT_FOUND, which mean the
    dependency answered correctly and the record genuinely has no match. This
    means we do not KNOW whether a match exists -- so the claims cannot be
    rejected on the strength of it.
    """


class PublishError(ClaimsPipelineError):
    """Kafka publishing did not complete after all configured attempts.

    Requirements section 4: after retry exhaustion, fail the DAG and alert.
    Critically, this must leave the input file UNARCHIVED so the run can be
    repeated for the same logical date once the broker is healthy.
    """


class ReconciliationError(ClaimsPipelineError):
    """The counts do not add up, so the run cannot be declared successful.

    The data-quality requirements are arithmetic:
        successful + rejected == total input
        kafka_acknowledged   == successful
    A violation means we have lost track of records somewhere in the pipeline.
    That is never an acceptable state to archive an input file on.
    """


@dataclass
class StageResult:
    """What a stage returns: the work, the rejects, and the counts.

    Every stage returns one of these rather than a bare list, so the
    reconciliation at the end has per-stage numbers to check and the task can
    push a compact summary to XCom without carrying the records themselves.
    """

    records: list[dict[str, Any]] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def ok_count(self) -> int:
        return len(self.records)

    @property
    def rejected_count(self) -> int:
        return len(self.rejections)

    def summary(self) -> dict[str, Any]:
        """XCom-safe: counts and reasons, never the claim records themselves."""
        reasons: dict[str, int] = {}
        for r in self.rejections:
            reasons[r.reason.value] = reasons.get(r.reason.value, 0) + 1
        return {
            "ok": self.ok_count,
            "rejected": self.rejected_count,
            "reasons": reasons,
            **self.metrics,
        }
