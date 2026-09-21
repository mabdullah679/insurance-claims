"""Run-scoped staging on the shared volume.

WHY NOT XCOM. Airflow's XCom stores task return values in the metadata
database. Claim records are PII -- names, states, dates of birth reached
through enrichment -- and a daily file can carry far more rows than XCom is
designed to hold. So XCom carries counts and keys, and the records themselves
move through date-partitioned files that every task addresses by convention:

    data/staging/dt=YYYY-MM-DD/validated.jsonl
    data/staging/dt=YYYY-MM-DD/enriched.jsonl
    data/staging/dt=YYYY-MM-DD/scored.jsonl
    data/staging/dt=YYYY-MM-DD/rejections/<stage>.jsonl
    data/staging/dt=YYYY-MM-DD/metrics/<name>.json

The consequence worth knowing: re-running one task by hand works without
touching XCom, because the task finds its input by date rather than by pulling
a path from the previous task's return value.

JSONL rather than Parquet: the volumes here are a daily claims file, the data
is read once by the next task, and a human debugging a failed run at 3am can
read a line of it without a pyarrow install.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from claims.common.outcomes import Rejection
from claims.common.reconcile import RunTotals

log = logging.getLogger(__name__)


def _encode(v: Any) -> Any:
    if isinstance(v, Decimal):
        return {"__decimal__": str(v)}
    if isinstance(v, datetime):
        return {"__datetime__": v.isoformat()}
    if isinstance(v, date):
        return {"__date__": v.isoformat()}
    raise TypeError(f"cannot serialise {type(v).__name__}")


def _decode(obj: dict) -> Any:
    """Round-trip Decimal and date through JSON.

    Claim amounts are Decimal by the time they leave validation, and letting
    them decay into float across a task boundary would put binary floating
    point into a financial reconciliation.
    """
    if "__decimal__" in obj:
        return Decimal(obj["__decimal__"])
    if "__datetime__" in obj:
        return datetime.fromisoformat(obj["__datetime__"])
    if "__date__" in obj:
        return date.fromisoformat(obj["__date__"])
    return obj


class Staging:
    """Everything one run writes, addressed by its processing date."""

    def __init__(self, root: Path | str, processing_date: date):
        base = os.environ.get("CLAIMS_DATA_ROOT") or (Path(root) / "data")
        self.dir = Path(base) / "staging" / f"dt={processing_date.isoformat()}"
        self.processing_date = processing_date
        (self.dir / "rejections").mkdir(parents=True, exist_ok=True)
        (self.dir / "metrics").mkdir(parents=True, exist_ok=True)

    # ── the raw input ────────────────────────────────────────────────────
    def write_raw(self, raw: bytes) -> Path:
        p = self.dir / "input.csv"
        p.write_bytes(raw)
        return p

    def read_raw(self) -> bytes:
        p = self.dir / "input.csv"
        if not p.exists():
            raise FileNotFoundError(
                f"{p} is missing: the check_input_file task did not stage the "
                f"input for {self.processing_date}. Re-run that task first.")
        return p.read_bytes()

    # ── records between stages ───────────────────────────────────────────
    def write_stage(self, name: str, records: list[dict[str, Any]]) -> Path:
        p = self.dir / f"{name}.jsonl"
        with p.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, default=_encode) + "\n")
        return p

    def read_stage(self, name: str) -> list[dict[str, Any]]:
        p = self.dir / f"{name}.jsonl"
        if not p.exists():
            raise FileNotFoundError(
                f"{p} is missing: the task producing {name!r} for "
                f"{self.processing_date} did not run or did not complete.")
        with p.open(encoding="utf-8") as f:
            return [json.loads(line, object_hook=_decode)
                    for line in f if line.strip()]

    # ── rejections, one file per producing stage ─────────────────────────
    def write_rejections(self, stage: str, rejections: list[Rejection]) -> Path:
        p = self.dir / "rejections" / f"{stage}.jsonl"
        with p.open("w", encoding="utf-8") as f:
            for r in rejections:
                f.write(json.dumps(r.to_error_row(), default=_encode) + "\n")
        return p

    def read_all_rejections(self) -> list[dict[str, Any]]:
        """Every rejected record from every stage, in stage order."""
        rows: list[dict[str, Any]] = []
        for p in sorted((self.dir / "rejections").glob("*.jsonl")):
            with p.open(encoding="utf-8") as f:
                rows += [json.loads(line, object_hook=_decode)
                         for line in f if line.strip()]
        return rows

    def rejection_counts(self) -> tuple[int, int, dict[str, int]]:
        """(validation, enrichment, reasons) -- for reconciliation."""
        counts = {"validation": 0, "enrichment": 0}
        reasons: dict[str, int] = {}
        for stage in counts:
            p = self.dir / "rejections" / f"{stage}.jsonl"
            if not p.exists():
                continue
            with p.open(encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    counts[stage] += 1
                    reason = json.loads(line).get("rejection_reason", "UNKNOWN")
                    reasons[reason] = reasons.get(reason, 0) + 1
        return counts["validation"], counts["enrichment"], reasons

    # ── metrics ──────────────────────────────────────────────────────────
    def write_metrics(self, name: str, metrics: dict[str, Any]) -> Path:
        p = self.dir / "metrics" / f"{name}.json"
        p.write_text(json.dumps(metrics, default=_encode, indent=2), encoding="utf-8")
        return p

    def read_metrics(self, name: str) -> dict[str, Any]:
        p = self.dir / "metrics" / f"{name}.json"
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"), object_hook=_decode)

    def totals(self) -> RunTotals:
        """Assemble the run's counts from what each stage actually wrote.

        Counts are DERIVED from the staged artifacts rather than passed along
        through XCom. If a task was re-run and produced different numbers, the
        reconciliation sees the numbers that are really on disk.
        """
        validation_rejected, enrichment_rejected, reasons = self.rejection_counts()
        kafka = self.read_metrics("kafka")

        # Input rows: what validation actually read, which is the only count
        # that includes rows rejected before they became records.
        validated = self._count("validated.jsonl")
        scored = self._count("scored.jsonl")

        return RunTotals(
            input_records=validated + validation_rejected,
            validation_rejected=validation_rejected,
            enrichment_rejected=enrichment_rejected,
            successful=scored,
            kafka_attempted=int(kafka.get("attempted", 0)),
            kafka_acknowledged=int(kafka.get("acknowledged", 0)),
            kafka_failed=int(kafka.get("failed", 0)),
            kafka_retries=int(kafka.get("retries", 0)),
            dead_lettered=int(kafka.get("dead_lettered", 0)),
            rejection_reasons=reasons,
        )

    def _count(self, filename: str) -> int:
        p = self.dir / filename
        if not p.exists():
            return 0
        with p.open(encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
