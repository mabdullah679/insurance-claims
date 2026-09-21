"""The arithmetic that decides whether a run may be called successful.

Requirements section 4 states the data-quality rules as equations:

    successful + rejected        == total input records
    kafka_acknowledged           == successful claims
    every rejected claim         has a stored reason

This module is where those are enforced, and it is deliberately separate from
every stage that produces the numbers: a stage checking its own arithmetic is
a stage marking its own homework.

WHY IT GATES THE ARCHIVE. Requirement: "The original input file is archived
only after required work succeeds." If the counts do not reconcile we do not
know what happened to the missing records, and archiving would move the only
evidence out of the inbound bucket. So reconciliation runs BEFORE the archive
task, and raises on violation.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from claims.common.outcomes import ReconciliationError

log = logging.getLogger(__name__)


@dataclass
class RunTotals:
    """Every count a run produces, from input row to acknowledged event."""

    input_records: int = 0
    validation_rejected: int = 0
    enrichment_rejected: int = 0
    successful: int = 0
    kafka_attempted: int = 0
    kafka_acknowledged: int = 0
    kafka_failed: int = 0
    kafka_retries: int = 0
    dead_lettered: int = 0
    rejection_reasons: dict[str, int] = field(default_factory=dict)

    @property
    def rejected(self) -> int:
        return self.validation_rejected + self.enrichment_rejected

    @property
    def rejection_rate(self) -> float:
        return self.rejected / self.input_records if self.input_records else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "input_records": self.input_records,
            "successful": self.successful,
            "rejected": self.rejected,
            "validation_rejected": self.validation_rejected,
            "enrichment_rejected": self.enrichment_rejected,
            "rejection_rate": round(self.rejection_rate, 4),
            "kafka_attempted": self.kafka_attempted,
            "kafka_acknowledged": self.kafka_acknowledged,
            "kafka_failed": self.kafka_failed,
            "kafka_retries": self.kafka_retries,
            "dead_lettered": self.dead_lettered,
            "rejection_reasons": dict(self.rejection_reasons),
        }


def reconcile(totals: RunTotals, *, rejections_stored: int | None = None) -> dict[str, Any]:
    """Check every invariant. Raise on the first violation that matters.

    `rejections_stored` is the count the S3 error-folder write actually
    reported. It is checked against the rejection count rather than assumed,
    because "every rejected claim must be stored with a rejection reason" is a
    promise about a side effect, and the only honest way to verify a side
    effect is to ask what it did.
    """
    problems: list[str] = []

    if totals.successful + totals.rejected != totals.input_records:
        problems.append(
            f"successful ({totals.successful}) + rejected ({totals.rejected}) "
            f"= {totals.successful + totals.rejected}, but the input carried "
            f"{totals.input_records} record(s); {abs(totals.input_records - totals.successful - totals.rejected)} "
            f"record(s) are unaccounted for")

    if totals.kafka_acknowledged != totals.successful:
        problems.append(
            f"Kafka acknowledged {totals.kafka_acknowledged} message(s) but "
            f"{totals.successful} claim(s) were processed successfully; every "
            f"successful claim must have exactly one acknowledged publication")

    if rejections_stored is not None and rejections_stored != totals.rejected:
        problems.append(
            f"{totals.rejected} claim(s) were rejected but {rejections_stored} "
            f"were written to the error location; every rejected claim must be "
            f"stored with its reason")

    if problems:
        raise ReconciliationError(
            "run does not reconcile, refusing to report success:\n  - "
            + "\n  - ".join(problems))

    log.info("reconciled: %d input = %d successful + %d rejected; "
             "%d Kafka acknowledgement(s)",
             totals.input_records, totals.successful, totals.rejected,
             totals.kafka_acknowledged)
    return totals.as_dict()


def rejection_rate_breached(totals: RunTotals, threshold: float) -> bool:
    """Alert condition, NOT a failure condition.

    Section 5 lists "the rejected-record rate exceeds the agreed threshold" as
    an alert. A bad day for source data is something Jessica Patel should hear
    about promptly; it is not a reason to fail a DAG that correctly handled
    every record it was given.
    """
    return totals.input_records > 0 and totals.rejection_rate > threshold
