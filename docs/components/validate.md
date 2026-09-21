# `claims/validate/` — file and record validation

**Requirements 1-4.** Turns the raw S3 object into validated claim records,
rejecting bad rows individually and failing the run on bad files.

## The boundary this module exists to draw

| Level | Example | Response |
|---|---|---|
| **File** | Missing file, empty file, missing required **column** | **Raise.** No row is processable. |
| **Record** | Empty value, unparseable amount, duplicate ID | **Reject.** Goes to the error folder, run continues. |

A missing `claim_amount` **column** is a file problem. Rejecting all five rows
instead would report a successful run with a 100% rejection rate, which reads
as a data-quality issue when it is actually a broken export.

## Public surface

```python
parse_csv(raw: bytes | str) -> list[dict]     # raises InputFileError, SchemaError
validate(rows, *, expected_date=None) -> StageResult
```

`REQUIRED_COLUMNS = (claim_id, customer_id, policy_id, claim_amount, claim_date)`

## Rules that are decisions, not accidents

- **Duplicates: first occurrence wins.** Deterministic across re-runs, and it
  keeps one copy of a genuinely duplicated claim rather than dropping both.
  Later copies are rejected `DUPLICATE_CLAIM_ID`. (Open question Q3.)
- **Amounts are `Decimal` and must be positive.** Thousands separators and a
  leading currency symbol are tolerated because real exports carry them.
- **Off-date claims are processed with a warning, never rejected.**
  Late-reported claims are a business reality and nothing in the requirements
  asks us to reject them. (Q4.)
- **A BOM is stripped.** An Excel export would otherwise make the first column
  `﻿claim_id` and fail schema validation with a confusing message.

## Rejection reasons produced here

`MISSING_REQUIRED_FIELD`, `INVALID_CLAIM_AMOUNT`, `INVALID_CLAIM_DATE`,
`DUPLICATE_CLAIM_ID`

## If you change this

Keep `ok_count + rejected_count == len(rows)`. Everything downstream
reconciles against it. `tests/test_validate.py` asserts it for every rejection
path.
