"""Unit tests for consumer validation, normalization, and threshold alerts."""

from __future__ import annotations

import csv
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "consumer"))
sys.path.insert(0, str(PROJECT_ROOT / "producer"))

from app.validator import classify_alert, validate_event  # noqa: E402
from replay_producer import csv_row_to_event  # noqa: E402


def _make_payload(**overrides):
    base = {
        "run_id": "test-run-1",
        "event_id": "AN-2089657-0085",
        "station_id": "AN-011624",
        "station_name": "APOLLO OFFICE UNITS RADCLIVE RD GAWCOTT",
        "easting": 467993.0,
        "northing": 232851.0,
        "determinand": "BOD ATU",
        "determinand_code": "85",
        "result_qualifier": "<",
        "result_raw": "4",
        "unit": "mg/l",
        "material_type": "FINAL SEWAGE EFFLUENT",
        "is_compliance": "TRUE",
        "event_time": "2025-01-13T12:04:00Z",
        "produced_at": "2026-11-23T05:00:00+00:00",
    }
    base.update(overrides)
    return base


def test_real_sample_csv_rows_validate_and_reject_noise():
    sample_csv = PROJECT_ROOT / "data" / "2025-C_sample.csv"
    assert sample_csv.is_file()

    valid_count = 0
    coded_rejected = 0
    with sample_csv.open("r", encoding="utf-8", newline="") as handle:
        for seq, row in enumerate(csv.DictReader(handle)):
            _, payload = csv_row_to_event(row, run_id="sample-run", seq=seq)
            res = validate_event(payload)
            if res.is_valid:
                valid_count += 1
            else:
                assert res.rejected is not None
                if res.rejected.reason == "NON_MEASUREMENT_CODED":
                    coded_rejected += 1

    assert valid_count > 0
    assert coded_rejected > 0


def test_reject_coded_no_flow_event():
    res = validate_event(_make_payload(determinand="NO FLOW/SAMP", unit="coded", result_raw="0"))
    assert not res.is_valid
    assert res.rejected is not None
    assert res.rejected.reason == "NON_MEASUREMENT_CODED"


def test_reject_non_numeric_nan_and_negative_values():
    for bad in ("abc", "", "NaN", "inf"):
        res = validate_event(_make_payload(result_raw=bad))
        assert not res.is_valid
        assert res.rejected is not None
        assert res.rejected.reason == "INVALID_RESULT_VALUE"

    neg_bod = validate_event(_make_payload(determinand="BOD ATU", result_raw="-1.5"))
    assert not neg_bod.is_valid
    assert neg_bod.rejected is not None
    assert neg_bod.rejected.reason == "NEGATIVE_VALUE"

    # Sub-zero water temperature is physically valid down to -5 C.
    cold_water = validate_event(_make_payload(determinand="Temp Water", unit="cel", result_raw="-0.5"))
    assert cold_water.is_valid


def test_reject_out_of_physical_range_ph_and_temp():
    bad_ph = validate_event(_make_payload(determinand="pH", unit="phunits", result_raw="15.2"))
    assert not bad_ph.is_valid
    assert bad_ph.rejected is not None
    assert bad_ph.rejected.reason == "OUT_OF_PHYSICAL_RANGE"

    bad_temp = validate_event(_make_payload(determinand="Temp Water", unit="cel", result_raw="85.0"))
    assert not bad_temp.is_valid
    assert bad_temp.rejected is not None
    assert bad_temp.rejected.reason == "OUT_OF_PHYSICAL_RANGE"


def test_threshold_alert_boundaries():
    assert classify_alert("BOD ATU", 25.0) == "NORMAL"
    assert classify_alert("BOD ATU", 25.1) == "WARNING"
    assert classify_alert("BOD ATU", 50.0) == "WARNING"
    assert classify_alert("BOD ATU", 50.1) == "CRITICAL"

    assert classify_alert("Ammonia(N)", 5.0) == "NORMAL"
    assert classify_alert("Ammonia(N)", 6.5) == "WARNING"
    assert classify_alert("Ammonia(N)", 16.0) == "CRITICAL"

    assert classify_alert("pH", 7.2) == "NORMAL"
    assert classify_alert("pH", 5.5) == "WARNING"
    assert classify_alert("pH", 4.9) == "CRITICAL"
    assert classify_alert("pH", 10.5) == "CRITICAL"


def test_timestamp_normalization_naive_to_utc():
    res = validate_event(_make_payload(event_time="2025-03-07T11:30:00", produced_at="2026-11-23T05:00:00"))
    assert res.is_valid
    assert res.valid is not None
    assert res.valid.event_time.utcoffset().total_seconds() == 0
    assert res.valid.produced_at.utcoffset().total_seconds() == 0
