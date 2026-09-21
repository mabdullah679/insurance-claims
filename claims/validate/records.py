"""File and record validation -- requirements 1 through 4.

The stage draws the reject/fail line twice, at two different levels, and the
distinction is the whole point:

  FILE level  -> raise. Missing file, empty file, or a missing required COLUMN
                 means no row in the file can be processed. Rejecting every
                 row would produce a "successful" run with a 100% rejection
                 rate, which reads as a data-quality problem when it is
                 actually an upstream export problem.

  RECORD level -> reject. A row missing a value, carrying an unparseable
                 amount or date, or duplicating another row's claim ID is a
                 handled business outcome: it goes to the error folder with a
                 reason, and the DAG carries on.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from claims.common.outcomes import (InputFileError, Rejection, RejectionReason,
                                    SchemaError, StageResult)

log = logging.getLogger(__name__)

# Requirement 3. These are the columns without which a claim is not a claim.
REQUIRED_COLUMNS = ("claim_id", "customer_id", "policy_id",
                    "claim_amount", "claim_date")

# Accepted date spellings, most likely first. The QA fixture carries ISO; real
# exports drift, and failing a whole day's file over a slash is not useful.
_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d")


def parse_csv(raw: bytes | str, *, encoding: str = "utf-8-sig") -> list[dict[str, str]]:
    """Bytes from S3 to a list of row dicts, with file-level checks.

    `utf-8-sig` by default because a BOM from an Excel export would otherwise
    make the first column name `﻿claim_id` and fail schema validation with
    a confusing message.
    """
    if isinstance(raw, bytes):
        try:
            text = raw.decode(encoding)
        except UnicodeDecodeError as e:
            raise InputFileError(f"claims file is not valid {encoding}: {e}") from e
    else:
        text = raw

    # A str input has not been through `utf-8-sig` decoding, so a BOM that
    # survived an earlier decode is still sitting on the first column name.
    # Strip it here rather than only in the bytes path: otherwise the same
    # file parses differently depending on who read it off S3.
    text = text.lstrip("﻿")

    if not text.strip():
        # Requirement 2. An empty file is a failure, not zero rejections:
        # Emily's upload did not carry the day's claims.
        raise InputFileError("claims file is empty")

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise InputFileError("claims file has no header row")

    header = [(h or "").strip() for h in reader.fieldnames]
    _assert_required_columns(header)

    rows = [{(k or "").strip(): (v.strip() if isinstance(v, str) else v)
             for k, v in row.items() if k is not None}
            for row in reader]
    if not rows:
        raise InputFileError("claims file has a header but no data rows")
    return rows


def _assert_required_columns(header: Iterable[str]) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in set(header)]
    if missing:
        raise SchemaError(
            f"claims file is missing required column(s): {', '.join(missing)}. "
            f"Present: {', '.join(header)}")


def validate(rows: list[dict[str, str]], *, expected_date: date | None = None) -> StageResult:
    """Validate every row, rejecting individually. Requirements 3 and 4.

    Duplicates are resolved by keeping the FIRST occurrence and rejecting every
    later one. That is a deliberate, stated choice: it is deterministic, so a
    re-run of the same file publishes the same claim rather than whichever row
    happened to land last, and it keeps one copy of a genuinely duplicated
    claim in the stream rather than dropping both.

    `expected_date` is checked only as a WARNING, never a rejection -- a claim
    dated outside the processing day is a real business case (late-reported
    claims) and the requirements never ask for it to be rejected.
    """
    seen: dict[str, int] = {}
    ok: list[dict[str, Any]] = []
    rejections: list[Rejection] = []
    off_date = 0

    for idx, row in enumerate(rows):
        claim_id = (row.get("claim_id") or "").strip()

        missing = [c for c in REQUIRED_COLUMNS if not (row.get(c) or "").strip()]
        if missing:
            rejections.append(Rejection(
                reason=RejectionReason.MISSING_REQUIRED_FIELD,
                detail=f"row {idx + 2}: empty required field(s): {', '.join(missing)}",
                record=row,
                claim_id=claim_id or None))
            continue

        if claim_id in seen:
            rejections.append(Rejection(
                reason=RejectionReason.DUPLICATE_CLAIM_ID,
                detail=(f"row {idx + 2}: claim_id {claim_id} already appeared at "
                        f"row {seen[claim_id] + 2}; keeping the first occurrence"),
                record=row,
                claim_id=claim_id))
            continue

        amount = _parse_amount(row.get("claim_amount"))
        if amount is None:
            rejections.append(Rejection(
                reason=RejectionReason.INVALID_CLAIM_AMOUNT,
                detail=(f"row {idx + 2}: claim_amount "
                        f"{row.get('claim_amount')!r} is not a positive number"),
                record=row,
                claim_id=claim_id))
            continue

        claim_date = _parse_date(row.get("claim_date"))
        if claim_date is None:
            rejections.append(Rejection(
                reason=RejectionReason.INVALID_CLAIM_DATE,
                detail=(f"row {idx + 2}: claim_date {row.get('claim_date')!r} "
                        f"is not a recognised date"),
                record=row,
                claim_id=claim_id))
            continue

        seen[claim_id] = idx
        if expected_date is not None and claim_date != expected_date:
            off_date += 1

        ok.append({
            **row,
            "claim_id": claim_id,
            "customer_id": (row.get("customer_id") or "").strip(),
            "policy_id": (row.get("policy_id") or "").strip(),
            "claim_amount": amount,
            "claim_date": claim_date,
        })

    if off_date:
        # Visible in the task log and the run metrics, but not an alert and
        # never a rejection.
        log.warning("%d claim(s) are not dated %s; processing them anyway",
                    off_date, expected_date)

    result = StageResult(records=ok, rejections=rejections)
    result.metrics = {
        "input_rows": len(rows),
        "duplicate_claim_ids": sum(
            1 for r in rejections if r.reason is RejectionReason.DUPLICATE_CLAIM_ID),
        "off_expected_date": off_date,
    }
    return result


def _parse_amount(raw: Any) -> Decimal | None:
    """Currency as Decimal. A claim amount must be a positive number.

    Decimal rather than float: these are monetary values that get summed into
    a daily total and compared against coverage limits, and float drift in a
    financial reconciliation is not worth the convenience. Thousands
    separators and a leading currency symbol are tolerated because exports
    carry them.
    """
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").lstrip("$").strip()
    if not s:
        return None
    try:
        value = Decimal(s)
    except (InvalidOperation, ValueError):
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


def _parse_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()
    if not s:
        return None
    # An ISO timestamp ("2026-09-11T00:00:00") is a date with extra precision.
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s[:10] if fmt == "%Y-%m-%d" else s, fmt).date()
        except ValueError:
            continue
    return None
