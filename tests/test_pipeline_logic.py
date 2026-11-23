"""Unit and smoke tests for producer mapping, UNNEST DB writer, evaluation, and charts."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "consumer"))
sys.path.insert(0, str(PROJECT_ROOT / "producer"))
sys.path.insert(0, str(PROJECT_ROOT / "benchmarks"))

from app.db import DatabaseWriter, VALID_BATCH_INSERT_SQL  # noqa: E402
from app.validator import validate_event  # noqa: E402
from evaluate_run import compute_run_metrics  # noqa: E402
from generate_charts import (  # noqa: E402
    render_recovery_chart,
    render_throughput_chart,
    sample_preview_data,
)
from replay_producer import csv_row_to_event  # noqa: E402


class _FakeCursor:
    def __init__(self, returned_ids):
        self.returned_ids = returned_ids
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return [(eid,) for eid in self.returned_ids]


class _FakeConnection:
    def __init__(self, returned_ids):
        self.cursor_obj = _FakeCursor(returned_ids)
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def test_csv_row_to_event_key_and_deterministic_id():
    row = {
        "@id": "http://environment.data.gov.uk/water-quality/data/measurement/AN-2089657-0085",
        "sample.samplingPoint.notation": "AN-011624",
        "sample.samplingPoint.label": "APOLLO OFFICE UNITS",
        "sample.sampleDateTime": "2025-01-13T12:04:00",
        "determinand.label": "BOD ATU",
        "determinand.notation": "85",
        "resultQualifier.notation": "<",
        "result": "4",
        "determinand.unit.label": "mg/l",
        "sample.sampledMaterialType.label": "FINAL SEWAGE EFFLUENT",
        "sample.isComplianceSample": "TRUE",
        "sample.samplingPoint.easting": "467993",
        "sample.samplingPoint.northing": "232851",
    }

    key0, ev0 = csv_row_to_event(row, run_id="r1", seq=0, loop_index=0, produced_at="2026-11-23T05:00:00Z")
    assert key0 == "AN-011624"
    assert ev0["event_id"] == "AN-2089657-0085"
    assert ev0["event_time"] == "2025-01-13T12:04:00Z"

    _, ev_loop = csv_row_to_event(row, run_id="r1", seq=50, loop_index=2, produced_at="2026-11-23T05:00:00Z")
    assert ev_loop["event_id"] == "AN-2089657-0085-loop2"


def test_database_writer_unnest_dedup_counting():
    v1 = validate_event(
        {
            "run_id": "r1",
            "event_id": "e-1",
            "station_id": "s-1",
            "determinand": "BOD ATU",
            "result_raw": "12",
            "event_time": "2025-01-13T12:04:00Z",
            "produced_at": "2026-11-23T05:00:00Z",
        }
    ).valid
    v2 = validate_event(
        {
            "run_id": "r1",
            "event_id": "e-2",
            "station_id": "s-1",
            "determinand": "Ammonia(N)",
            "result_raw": "8",
            "event_time": "2025-01-13T12:05:00Z",
            "produced_at": "2026-11-23T05:00:01Z",
        }
    ).valid
    assert v1 is not None and v2 is not None

    # Simulate e-1 already committed before crash, so only e-2 is newly returned.
    fake_conn = _FakeConnection(returned_ids=["e-2"])
    writer = DatabaseWriter("postgresql://unused", connect_fn=lambda _url: fake_conn)
    inserted_valid, deduped_valid, inserted_rejected = writer.write_batch([v1, v2], [])

    assert fake_conn.committed is True
    assert inserted_valid == 1
    assert deduped_valid == 1
    assert inserted_rejected == 0
    executed_sql, executed_params = fake_conn.cursor_obj.executed[0]
    assert "ON CONFLICT (run_id, event_id) DO NOTHING" in executed_sql
    assert executed_sql == VALID_BATCH_INSERT_SQL
    assert len(executed_params) == 16
    assert executed_params[1] == ["e-1", "e-2"]


def test_compute_run_metrics():
    metrics = compute_run_metrics(
        run_id="sustained-batch-1",
        phase="sustained-batch",
        db_stats={
            "valid_rows": 980,
            "distinct_valid": 980,
            "rejected_rows": 20,
            "drain_span_s": 10.0,
            "p50_ms": 210.4,
            "p95_ms": 540.2,
            "max_ms": 890.0,
            "alerts": {"NORMAL": 900, "WARNING": 60, "CRITICAL": 20},
            "first_event_id": "AN-1",
        },
        producer_report={"actual_rate_eps": 100.1, "elapsed_seconds": 9.99},
        kafka_lag=0,
        expected_valid=980,
    )
    assert metrics["duplicate_rows"] == 0
    assert metrics["total_db_rows"] == 1000
    assert metrics["db_throughput_eps"] == 100.0
    assert metrics["kafka_lag"] == 0


def test_generate_preview_charts_smoke(tmp_path: Path):
    data = sample_preview_data()
    t_chart = tmp_path / "slide9.png"
    r_chart = tmp_path / "slide10.png"

    render_throughput_chart(data, t_chart)
    render_recovery_chart(data, r_chart)

    assert t_chart.is_file() and t_chart.stat().st_size > 20_000
    assert r_chart.is_file() and r_chart.stat().st_size > 20_000
