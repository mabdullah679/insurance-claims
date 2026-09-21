# `claims/enrich/` — customer and policy join

**Requirements 6-8.** Joins each validated claim to its customer record from
PostgreSQL and its policy from the REST API.

## The distinction this module protects

```
"customer CUS-QA-1001 does not exist"   ->  reject that one claim
"PostgreSQL refused the connection"     ->  fail the DAG
```

Both look like an absent customer record if you are careless. Conflating them
is a silent-data-loss bug: a five-minute database blip would reject every
claim in the run, write them to the error folder as if the **data** were bad,
report success, and archive the input file.

So the lookup protocols are contracted to:

- **raise `DependencyError`** when they cannot answer, and
- **return a dict of found records** when they can.

Absence in a successful answer is a rejection. An exception is a failure. A
gateway that cannot tell the two apart must raise. A lookup returning `None`
is itself treated as a failure, because it has confused the two.

## Public surface

```python
run(claims, customers: CustomerLookup, policies: PolicyLookup,
    *, as_of: date | None = None, require_active_policy: bool = True) -> StageResult
```

Both lookups are `Protocol`s, so tests inject fakes and production injects the
real gateways. Lookups are **batched**, never per-claim: one `IN` query and
paged API calls, because the requirements set an operational completion
window.

## Policy acceptance

A claim is rejected `POLICY_NOT_ACTIVE` when the policy status is not
`ACTIVE`, or when the claim date falls outside the policy's effective and
expiration window. Configurable off with `require_active_policy=False`.

Open question Q5: are `PENDING_RENEWAL` or `GRACE_PERIOD` also acceptable?

## Fields added to each claim

`customer_name`, `customer_state`, `customer_status`, `customer_since`,
`customer_tenure_years`, `policy_type`, `coverage_limit`, `policy_status`,
`policy_effective_date`, `policy_expiration_date`

Source fields are namespaced rather than blindly merged, so a `state` column
in the claims export cannot overwrite the customer's state.

## Rejection reasons produced here

`CUSTOMER_NOT_FOUND`, `POLICY_NOT_FOUND`, `POLICY_NOT_ACTIVE`

## If you change this

Never catch `DependencyError` here. `tests/test_enrich.py::TestOutageIsNotRejection`
is the guard, and it is the most important test class in the suite.
