"""Validation: the file/record failure boundary, requirements 1-4.

The tests are organised around the distinction the requirements draw, because
that is what is most likely to regress: a future change that "helpfully"
catches a SchemaError and rejects the rows instead would pass a naive test
suite and silently turn a broken export into a successful run.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from claims.common.outcomes import InputFileError, RejectionReason, SchemaError
from claims.validate import records as v
from tests.conftest import QA_CSV, QA_DATE


class TestFileLevelFailures:
    """These must RAISE. Rejecting instead would report a successful run."""

    def test_empty_file_raises(self):
        with pytest.raises(InputFileError, match="empty"):
            v.parse_csv("")

    def test_whitespace_only_file_raises(self):
        with pytest.raises(InputFileError, match="empty"):
            v.parse_csv("   \n  \n")

    def test_header_with_no_rows_raises(self):
        with pytest.raises(InputFileError, match="no data rows"):
            v.parse_csv("claim_id,customer_id,policy_id,claim_amount,claim_date\n")

    def test_missing_required_column_raises_not_rejects(self):
        """A missing COLUMN is a file problem, not five record problems."""
        csv = ("claim_id,customer_id,policy_id,claim_date\n"
               "C1,CUS1,POL1,2026-09-11\n")
        with pytest.raises(SchemaError, match="claim_amount"):
            v.parse_csv(csv)

    def test_schema_error_names_every_missing_column(self):
        with pytest.raises(SchemaError) as e:
            v.parse_csv("claim_id,customer_id\nC1,CUS1\n")
        msg = str(e.value)
        assert "policy_id" in msg and "claim_amount" in msg and "claim_date" in msg

    def test_bom_does_not_break_schema_validation(self):
        """An Excel export carries a BOM; that must not look like a bad schema."""
        rows = v.parse_csv("﻿" + QA_CSV)
        assert len(rows) == 5
        assert rows[0]["claim_id"] == "CLM-QA-20260911-001"


class TestHappyPath:
    def test_qa_fixture_produces_five_valid_claims(self):
        result = v.validate(v.parse_csv(QA_CSV), expected_date=QA_DATE)
        assert result.ok_count == 5
        assert result.rejected_count == 0

    def test_amount_is_decimal_not_float(self):
        """Monetary values get summed and compared; float drift is unacceptable."""
        result = v.validate(v.parse_csv(QA_CSV))
        amounts = [c["claim_amount"] for c in result.records]
        assert all(isinstance(a, Decimal) for a in amounts)
        assert Decimal("1250.75") in amounts

    def test_claim_date_is_a_date(self):
        result = v.validate(v.parse_csv(QA_CSV))
        assert all(isinstance(c["claim_date"], date) for c in result.records)


class TestRecordLevelRejections:
    """These must REJECT, not raise: the DAG still succeeds."""

    def _rows(self, *extra):
        base = ("claim_id,customer_id,policy_id,claim_amount,claim_date\n"
                "C1,CUS1,POL1,100.00,2026-09-11\n")
        return v.parse_csv(base + "".join(extra))

    def test_missing_value_is_rejected(self):
        result = v.validate(self._rows("C2,,POL2,200.00,2026-09-11\n"))
        assert result.ok_count == 1
        assert result.rejected_count == 1
        assert result.rejections[0].reason is RejectionReason.MISSING_REQUIRED_FIELD
        assert "customer_id" in result.rejections[0].detail

    def test_non_numeric_amount_is_rejected(self):
        result = v.validate(self._rows("C2,CUS2,POL2,not-a-number,2026-09-11\n"))
        assert result.rejections[0].reason is RejectionReason.INVALID_CLAIM_AMOUNT

    @pytest.mark.parametrize("amount", ["0", "-50.00", "0.00"])
    def test_non_positive_amount_is_rejected(self, amount):
        result = v.validate(self._rows(f"C2,CUS2,POL2,{amount},2026-09-11\n"))
        assert result.rejections[0].reason is RejectionReason.INVALID_CLAIM_AMOUNT

    def test_unparseable_date_is_rejected(self):
        result = v.validate(self._rows("C2,CUS2,POL2,200.00,not-a-date\n"))
        assert result.rejections[0].reason is RejectionReason.INVALID_CLAIM_DATE

    def test_currency_formatting_is_tolerated(self):
        """Exports carry "$1,250.75"; that is formatting, not bad data."""
        result = v.validate(self._rows('C2,CUS2,POL2,"$1,250.75",2026-09-11\n'))
        assert result.ok_count == 2
        assert result.records[1]["claim_amount"] == Decimal("1250.75")


class TestDuplicates:
    """Requirement 4, and the resolution rule the design commits to."""

    def test_duplicate_claim_id_is_rejected(self):
        csv = ("claim_id,customer_id,policy_id,claim_amount,claim_date\n"
               "C1,CUS1,POL1,100.00,2026-09-11\n"
               "C1,CUS9,POL9,999.00,2026-09-11\n")
        result = v.validate(v.parse_csv(csv))
        assert result.ok_count == 1
        assert result.rejected_count == 1
        assert result.rejections[0].reason is RejectionReason.DUPLICATE_CLAIM_ID

    def test_first_occurrence_wins_deterministically(self):
        """A re-run must publish the same row, not whichever landed last."""
        csv = ("claim_id,customer_id,policy_id,claim_amount,claim_date\n"
               "C1,CUS1,POL1,100.00,2026-09-11\n"
               "C1,CUS9,POL9,999.00,2026-09-11\n")
        result = v.validate(v.parse_csv(csv))
        assert result.records[0]["customer_id"] == "CUS1"
        assert result.records[0]["claim_amount"] == Decimal("100.00")

    def test_triplicate_rejects_two(self):
        csv = ("claim_id,customer_id,policy_id,claim_amount,claim_date\n"
               + "C1,CUS1,POL1,100.00,2026-09-11\n" * 3)
        result = v.validate(v.parse_csv(csv))
        assert result.ok_count == 1
        assert result.rejected_count == 2


class TestReconciliationHolds:
    """The invariant that makes the whole run checkable."""

    @pytest.mark.parametrize("csv_body", [
        "C2,,POL2,200.00,2026-09-11\n",
        "C2,CUS2,POL2,bad,2026-09-11\n",
        "C1,CUS9,POL9,999.00,2026-09-11\n",
        "C2,CUS2,POL2,200.00,bad-date\n",
    ])
    def test_valid_plus_rejected_always_equals_input(self, csv_body):
        base = ("claim_id,customer_id,policy_id,claim_amount,claim_date\n"
                "C1,CUS1,POL1,100.00,2026-09-11\n")
        rows = v.parse_csv(base + csv_body)
        result = v.validate(rows)
        assert result.ok_count + result.rejected_count == len(rows)

    def test_every_rejection_carries_a_reason_and_the_record(self):
        rows = v.parse_csv(
            "claim_id,customer_id,policy_id,claim_amount,claim_date\n"
            "C1,CUS1,POL1,bad,2026-09-11\n")
        result = v.validate(rows)
        r = result.rejections[0]
        assert r.reason and r.detail and r.record
        assert r.to_error_row()["rejection_reason"] == r.reason.value


class TestOffDateClaims:
    def test_claim_dated_outside_the_run_is_processed_not_rejected(self):
        """Late-reported claims are a business reality, not a data error."""
        csv = ("claim_id,customer_id,policy_id,claim_amount,claim_date\n"
               "C1,CUS1,POL1,100.00,2026-09-01\n")
        result = v.validate(v.parse_csv(csv), expected_date=QA_DATE)
        assert result.ok_count == 1
        assert result.metrics["off_expected_date"] == 1
