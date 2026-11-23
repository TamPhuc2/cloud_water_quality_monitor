"""Replay UK Environment Agency 2025 water-quality CSV rows into Kafka."""

from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from dotenv import load_dotenv


CORE_DETERMINANDS = frozenset(
    {
        "BOD ATU",
        "Sld Sus@105C",
        "Ammonia(N)",
        "COD as O2",
        "Phosphorus-P",
        "pH",
        "Temp Water",
        "NO FLOW/SAMP",
    }
)


def default_csv_path() -> Path:
    """Resolve the full 2025 CSV dataset, falling back to the sample file."""
    data_dir = Path(__file__).resolve().parent.parent.parent / "Đánh giá mô hình"
    full_csv = data_dir / "2025-C - Copy.csv"
    if full_csv.is_file():
        return full_csv
    return data_dir / "2025-C_sample.csv"


def _normalize_event_time(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    # Naive ISO timestamps from UK EA CSV are treated as UTC for consistent storage.
    if text.endswith("Z") or "+" in text[10:] or "-" in text[10:]:
        return text
    return f"{text}Z"


def _parse_float_or_none(raw: str) -> float | None:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def csv_row_to_event(
    row: dict[str, str],
    run_id: str,
    seq: int,
    loop_index: int = 0,
    produced_at: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Transform a CSV row into (partition_key, event_payload)."""
    raw_id = (row.get("@id") or "").strip().rstrip("/")
    tail_id = raw_id.rsplit("/", 1)[-1] if raw_id else f"row-{seq:06d}"
    event_id = f"{tail_id}-loop{loop_index}" if loop_index > 0 else tail_id

    station_id = (row.get("sample.samplingPoint.notation") or "").strip() or "UNKNOWN-STATION"
    station_name = (row.get("sample.samplingPoint.label") or "").strip() or station_id

    payload: dict[str, Any] = {
        "run_id": run_id,
        "event_id": event_id,
        "seq": seq,
        "station_id": station_id,
        "station_name": station_name,
        "easting": _parse_float_or_none(row.get("sample.samplingPoint.easting") or ""),
        "northing": _parse_float_or_none(row.get("sample.samplingPoint.northing") or ""),
        "determinand": (row.get("determinand.label") or "").strip(),
        "determinand_code": (row.get("determinand.notation") or "").strip(),
        "result_qualifier": (row.get("resultQualifier.notation") or "").strip(),
        "result_raw": (row.get("result") or "").strip(),
        "unit": (row.get("determinand.unit.label") or "").strip(),
        "material_type": (row.get("sample.sampledMaterialType.label") or "").strip(),
        "is_compliance": (row.get("sample.isComplianceSample") or "").strip(),
        "event_time": _normalize_event_time(row.get("sample.sampleDateTime") or ""),
        "produced_at": produced_at or datetime.now(timezone.utc).isoformat(),
    }
    return station_id, payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay 2025 water-quality CSV to Kafka.")
    parser.add_argument("--csv", default=str(default_csv_path()), help="Path to CSV input file.")
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
        help="Kafka bootstrap servers (default: 127.0.0.1:19092 via SSH tunnel).",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "water-quality-events"),
        help="Target Kafka topic.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Logical run identifier (auto-generated UTC timestamp if omitted).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=20.0,
        help="Target events per second (0 = unlimited burst).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum events to publish (0 = entire dataset or infinite with --loop).",
    )
    parser.add_argument(
        "--core-only",
        action="store_true",
        default=True,
        help="Filter to core 8 water-quality determinands (default: True).",
    )
    parser.add_argument(
        "--all-determinands",
        action="store_false",
        dest="core_only",
        help="Include all 483 determinands from the CSV.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Wrap around CSV continuously until --max-events or Ctrl+C.",
    )
    parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON summary report.",
    )
    return parser.parse_args()


def _iter_filtered_rows(csv_path: Path, core_only: bool, loop: bool):
    loop_index = 0
    while True:
        yielded_this_pass = 0
        with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                det = (row.get("determinand.label") or "").strip()
                if core_only and det not in CORE_DETERMINANDS:
                    continue
                yielded_this_pass += 1
                yield loop_index, row
        if not loop or yielded_this_pass == 0:
            break
        loop_index += 1


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"ERROR: CSV file not found: {csv_path}", file=sys.stderr)
        return 1

    run_id = args.run_id or f"run-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "lz4",
            "retries": 5,
        }
    )

    stop_requested = False

    def _on_signal(_signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    delivered = 0
    delivery_errors = 0

    def _delivery_cb(err: Any, _msg: Any) -> None:
        nonlocal delivered, delivery_errors
        if err is not None:
            delivery_errors += 1
        else:
            delivered += 1

    print(
        f"Starting replay: run_id={run_id} csv={csv_path.name} "
        f"bootstrap={args.bootstrap} topic={args.topic} rate={args.rate} "
        f"max_events={args.max_events or 'ALL'} core_only={args.core_only} loop={args.loop}",
        flush=True,
    )

    start_wall = datetime.now(timezone.utc)
    start_mono = time.monotonic()
    last_progress = start_mono
    sent = 0
    coded_noise = 0

    try:
        for loop_idx, row in _iter_filtered_rows(csv_path, args.core_only, args.loop):
            if stop_requested:
                break
            if args.max_events > 0 and sent >= args.max_events:
                break

            if args.rate > 0:
                target_t = start_mono + (sent / args.rate)
                sleep_s = target_t - time.monotonic()
                if sleep_s > 0:
                    time.sleep(sleep_s)

            key, payload = csv_row_to_event(row, run_id=run_id, seq=sent, loop_index=loop_idx)
            if payload["determinand"] == "NO FLOW/SAMP" or payload["unit"].lower() == "coded":
                coded_noise += 1

            raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    producer.produce(
                        topic=args.topic,
                        key=key.encode("utf-8"),
                        value=raw_bytes,
                        on_delivery=_delivery_cb,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)

            sent += 1
            producer.poll(0)

            now_mono = time.monotonic()
            if now_mono - last_progress >= 5.0:
                elapsed = now_mono - start_mono
                actual_rate = sent / elapsed if elapsed > 0 else 0.0
                print(
                    f"[{run_id}] sent={sent} delivered={delivered} errors={delivery_errors} "
                    f"rate={actual_rate:.2f} evt/s",
                    flush=True,
                )
                last_progress = now_mono
    finally:
        remaining = producer.flush(timeout=30.0)
        if remaining > 0:
            print(f"WARNING: {remaining} messages remained unflushed after timeout.", file=sys.stderr)

    elapsed_s = max(time.monotonic() - start_mono, 1e-6)
    end_wall = datetime.now(timezone.utc)
    actual_eps = sent / elapsed_s

    report = {
        "run_id": run_id,
        "csv_path": str(csv_path),
        "bootstrap": args.bootstrap,
        "topic": args.topic,
        "target_rate_eps": args.rate,
        "actual_rate_eps": round(actual_eps, 3),
        "sent_events": sent,
        "delivered_events": delivered,
        "delivery_errors": delivery_errors,
        "coded_noise_events": coded_noise,
        "expected_valid_events": max(0, sent - coded_noise),
        "elapsed_seconds": round(elapsed_s, 3),
        "started_at": start_wall.isoformat(),
        "finished_at": end_wall.isoformat(),
    }

    print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)

    if args.report_json:
        out_path = Path(args.report_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    return 0 if delivery_errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
