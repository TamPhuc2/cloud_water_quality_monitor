"""Idempotent PostgreSQL / Neon batch writer with reconnect backoff."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .validator import RejectedMeasurement, ValidMeasurement


VALID_BATCH_INSERT_SQL = """
INSERT INTO water_measurements (
    run_id,
    event_id,
    station_id,
    station_name,
    easting,
    northing,
    determinand,
    determinand_code,
    result_qualifier,
    result_value,
    unit,
    material_type,
    is_compliance,
    alert_level,
    event_time,
    produced_at
)
SELECT *
FROM unnest(
    %s::text[],
    %s::text[],
    %s::text[],
    %s::text[],
    %s::double precision[],
    %s::double precision[],
    %s::text[],
    %s::text[],
    %s::text[],
    %s::double precision[],
    %s::text[],
    %s::text[],
    %s::boolean[],
    %s::text[],
    %s::timestamptz[],
    %s::timestamptz[]
)
ON CONFLICT (run_id, event_id) DO NOTHING
RETURNING event_id
"""

REJECTED_BATCH_INSERT_SQL = """
INSERT INTO rejected_events (
    run_id,
    event_id,
    station_id,
    determinand,
    reason,
    raw_payload
)
SELECT *
FROM unnest(
    %s::text[],
    %s::text[],
    %s::text[],
    %s::text[],
    %s::text[],
    %s::jsonb[]
)
ON CONFLICT (run_id, event_id) DO NOTHING
RETURNING event_id
"""


def pack_valid_columns(records: Sequence[ValidMeasurement]) -> tuple[list[Any], ...]:
    """Transpose ValidMeasurement records into typed column arrays for UNNEST."""
    return (
        [r.run_id for r in records],
        [r.event_id for r in records],
        [r.station_id for r in records],
        [r.station_name for r in records],
        [r.easting for r in records],
        [r.northing for r in records],
        [r.determinand for r in records],
        [r.determinand_code for r in records],
        [r.result_qualifier for r in records],
        [r.result_value for r in records],
        [r.unit for r in records],
        [r.material_type for r in records],
        [r.is_compliance for r in records],
        [r.alert_level for r in records],
        [r.event_time for r in records],
        [r.produced_at for r in records],
    )


def pack_rejected_columns(records: Sequence[RejectedMeasurement]) -> tuple[list[Any], ...]:
    """Transpose RejectedMeasurement records into typed column arrays for UNNEST."""
    return (
        [r.run_id for r in records],
        [r.event_id for r in records],
        [r.station_id for r in records],
        [r.determinand for r in records],
        [r.reason for r in records],
        [Jsonb(r.raw_payload) for r in records],
    )


def _default_connect(database_url: str) -> psycopg.Connection[Any]:
    # prepare_threshold=None is mandatory for Neon PgBouncer pooled endpoints.
    return psycopg.connect(
        database_url,
        prepare_threshold=None,
        connect_timeout=15,
        autocommit=False,
    )


class DatabaseWriter:
    """Manages a persistent PostgreSQL connection and atomic idempotent batch writes."""

    def __init__(
        self,
        database_url: str,
        connect_fn: Callable[[str], Any] | None = None,
        max_retries: int = 4,
    ) -> None:
        self._database_url = database_url
        self._connect_fn = connect_fn or _default_connect
        self._max_retries = max_retries
        self._conn: Any = None

    def _ensure_connection(self) -> Any:
        if self._conn is None or getattr(self._conn, "closed", False):
            self._conn = self._connect_fn(self._database_url)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def write_batch(
        self,
        valid_records: Sequence[ValidMeasurement],
        rejected_records: Sequence[RejectedMeasurement],
    ) -> tuple[int, int, int]:
        """Write valid & rejected records in one atomic transaction.

        Returns (inserted_valid, deduped_valid, inserted_rejected).
        """
        if not valid_records and not rejected_records:
            return (0, 0, 0)

        last_err: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                conn = self._ensure_connection()
                inserted_valid = 0
                inserted_rejected = 0

                with conn.cursor() as cur:
                    if valid_records:
                        cur.execute(VALID_BATCH_INSERT_SQL, pack_valid_columns(valid_records))
                        inserted_valid = len(cur.fetchall())

                    if rejected_records:
                        cur.execute(
                            REJECTED_BATCH_INSERT_SQL,
                            pack_rejected_columns(rejected_records),
                        )
                        inserted_rejected = len(cur.fetchall())

                conn.commit()
                deduped_valid = len(valid_records) - inserted_valid
                return (inserted_valid, deduped_valid, inserted_rejected)

            except psycopg.OperationalError as exc:
                last_err = exc
                self.close()
                backoff = min(2.0**attempt, 8.0)
                time.sleep(backoff)
            except Exception:
                if self._conn is not None:
                    try:
                        self._conn.rollback()
                    except Exception:
                        self.close()
                raise

        assert last_err is not None
        raise last_err
