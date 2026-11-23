"""Event validation, normalization, and threshold alert classification."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


# Thresholds tuned for UK Environment Agency water-quality determinands.
THRESHOLDS: dict[str, dict[str, float]] = {
    "BOD ATU": {"warning_high": 25.0, "critical_high": 50.0},
    "Ammonia(N)": {"warning_high": 5.0, "critical_high": 15.0},
    "Sld Sus@105C": {"warning_high": 35.0, "critical_high": 75.0},
    "COD as O2": {"warning_high": 125.0, "critical_high": 250.0},
    "Phosphorus-P": {"warning_high": 2.0, "critical_high": 5.0},
    "pH": {
        "warning_low": 6.0,
        "warning_high": 9.0,
        "critical_low": 5.0,
        "critical_high": 10.0,
    },
    "Temp Water": {"warning_high": 25.0, "critical_high": 30.0},
}

REQUIRED_FIELDS = (
    "run_id",
    "event_id",
    "station_id",
    "determinand",
    "event_time",
    "produced_at",
)


@dataclass(frozen=True)
class ValidMeasurement:
    run_id: str
    event_id: str
    station_id: str
    station_name: str
    easting: float | None
    northing: float | None
    determinand: str
    determinand_code: str | None
    result_qualifier: str | None
    result_value: float
    unit: str | None
    material_type: str | None
    is_compliance: bool | None
    alert_level: str
    event_time: datetime
    produced_at: datetime


@dataclass(frozen=True)
class RejectedMeasurement:
    run_id: str
    event_id: str
    station_id: str | None
    determinand: str | None
    reason: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class ValidationResult:
    valid: ValidMeasurement | None = None
    rejected: RejectedMeasurement | None = None

    @property
    def is_valid(self) -> bool:
        return self.valid is not None


def parse_iso_utc(raw: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp and normalize to UTC."""
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_optional_float(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val):
        return None
    return val


def _parse_optional_bool(raw: Any) -> bool | None:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return None
    text = str(raw).strip().upper()
    if text == "TRUE":
        return True
    if text == "FALSE":
        return False
    return None


def classify_alert(determinand: str, value: float) -> str:
    """Classify a numeric measurement into NORMAL, WARNING, or CRITICAL."""
    rule = THRESHOLDS.get(determinand)
    if not rule:
        return "NORMAL"

    crit_low = rule.get("critical_low")
    crit_high = rule.get("critical_high")
    if (crit_low is not None and value < crit_low) or (
        crit_high is not None and value > crit_high
    ):
        return "CRITICAL"

    warn_low = rule.get("warning_low")
    warn_high = rule.get("warning_high")
    if (warn_low is not None and value < warn_low) or (
        warn_high is not None and value > warn_high
    ):
        return "WARNING"

    return "NORMAL"


def validate_event(payload: dict[str, Any]) -> ValidationResult:
    """Validate and normalize a raw Kafka JSON payload."""
    if not isinstance(payload, dict):
        return ValidationResult(
            rejected=RejectedMeasurement(
                run_id="unknown",
                event_id="unknown",
                station_id=None,
                determinand=None,
                reason="INVALID_PAYLOAD_TYPE",
                raw_payload={"raw": str(payload)},
            )
        )

    run_id = str(payload.get("run_id") or "").strip() or "unknown"
    event_id = str(payload.get("event_id") or "").strip() or "unknown"
    station_id_raw = str(payload.get("station_id") or "").strip()
    determinand_raw = str(payload.get("determinand") or "").strip()

    def reject(reason: str) -> ValidationResult:
        return ValidationResult(
            rejected=RejectedMeasurement(
                run_id=run_id,
                event_id=event_id,
                station_id=station_id_raw or None,
                determinand=determinand_raw or None,
                reason=reason,
                raw_payload=payload,
            )
        )

    for field in REQUIRED_FIELDS:
        val = payload.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            return reject(f"MISSING_REQUIRED_FIELD:{field}")

    unit_raw = str(payload.get("unit") or "").strip()
    if determinand_raw == "NO FLOW/SAMP" or unit_raw.lower() == "coded":
        return reject("NON_MEASUREMENT_CODED")

    result_val = _parse_optional_float(payload.get("result_raw"))
    if result_val is None:
        return reject("INVALID_RESULT_VALUE")

    if determinand_raw != "Temp Water" and result_val < 0:
        return reject("NEGATIVE_VALUE")

    if determinand_raw == "pH" and not (0.0 <= result_val <= 14.0):
        return reject("OUT_OF_PHYSICAL_RANGE")

    if determinand_raw == "Temp Water" and not (-5.0 <= result_val <= 60.0):
        return reject("OUT_OF_PHYSICAL_RANGE")

    event_time = parse_iso_utc(payload.get("event_time"))
    if event_time is None:
        return reject("INVALID_EVENT_TIME")

    produced_at = parse_iso_utc(payload.get("produced_at"))
    if produced_at is None:
        return reject("INVALID_PRODUCED_AT")

    station_name = str(payload.get("station_name") or "").strip() or station_id_raw
    det_code = str(payload.get("determinand_code") or "").strip() or None
    qualifier = str(payload.get("result_qualifier") or "").strip() or None
    material_type = str(payload.get("material_type") or "").strip() or None

    return ValidationResult(
        valid=ValidMeasurement(
            run_id=run_id,
            event_id=event_id,
            station_id=station_id_raw,
            station_name=station_name,
            easting=_parse_optional_float(payload.get("easting")),
            northing=_parse_optional_float(payload.get("northing")),
            determinand=determinand_raw,
            determinand_code=det_code,
            result_qualifier=qualifier,
            result_value=result_val,
            unit=unit_raw or None,
            material_type=material_type,
            is_compliance=_parse_optional_bool(payload.get("is_compliance")),
            alert_level=classify_alert(determinand_raw, result_val),
            event_time=event_time,
            produced_at=produced_at,
        )
    )
