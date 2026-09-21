"""Update the daily claims summary table -- requirement 13.

Written as an UPSERT keyed on the processing date, because the requirements
explicitly permit "a corrected run to be executed for the same logical
processing date". A plain INSERT would leave two contradictory rows for one
day, and whichever the reporting layer happened to read would decide what
Emily Carter sees.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from claims.common.config import EnrichmentConfig
from claims.common.outcomes import DependencyError
from claims.common.reconcile import RunTotals

log = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    processing_date        DATE PRIMARY KEY,
    input_records          INTEGER NOT NULL,
    successful_claims      INTEGER NOT NULL,
    rejected_claims        INTEGER NOT NULL,
    kafka_acknowledged     INTEGER NOT NULL,
    kafka_retries          INTEGER NOT NULL,
    dead_lettered          INTEGER NOT NULL,
    rejection_rate         NUMERIC(6, 4) NOT NULL,
    dag_run_id             TEXT NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

UPSERT = """
INSERT INTO {table} (
    processing_date, input_records, successful_claims, rejected_claims,
    kafka_acknowledged, kafka_retries, dead_lettered, rejection_rate,
    dag_run_id, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (processing_date) DO UPDATE SET
    input_records      = EXCLUDED.input_records,
    successful_claims  = EXCLUDED.successful_claims,
    rejected_claims    = EXCLUDED.rejected_claims,
    kafka_acknowledged = EXCLUDED.kafka_acknowledged,
    kafka_retries      = EXCLUDED.kafka_retries,
    dead_lettered      = EXCLUDED.dead_lettered,
    rejection_rate     = EXCLUDED.rejection_rate,
    dag_run_id         = EXCLUDED.dag_run_id,
    updated_at         = NOW()
"""


def run(processing_date: date, totals: RunTotals, *, dag_run_id: str,
        cfg: EnrichmentConfig | None = None, hook: Any = None) -> dict[str, Any]:
    """Upsert one row for the processing date."""
    cfg = cfg or EnrichmentConfig()
    table = cfg.summary_table
    _assert_identifier(table)

    if hook is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=cfg.postgres_conn_id)

    params = (processing_date, totals.input_records, totals.successful,
              totals.rejected, totals.kafka_acknowledged, totals.kafka_retries,
              totals.dead_lettered, round(totals.rejection_rate, 4), dag_run_id)
    try:
        hook.run(DDL.format(table=table))
        hook.run(UPSERT.format(table=table), parameters=params)
    except Exception as e:
        raise DependencyError(f"could not update {table}: {e}") from e

    log.info("summary upserted for %s: %d successful, %d rejected",
             processing_date, totals.successful, totals.rejected)
    return {"table": table, "processing_date": processing_date.isoformat(),
            "rows_upserted": 1}


def _assert_identifier(name: str) -> None:
    parts = name.split(".")
    if not parts or not all(p.replace("_", "").isalnum() and p for p in parts):
        raise ValueError(f"unsafe SQL identifier in configuration: {name!r}")
