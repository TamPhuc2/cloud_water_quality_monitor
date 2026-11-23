"""Kafka consumer service: validates water-quality events and writes idempotently to DB."""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message
from dotenv import load_dotenv

from .db import DatabaseWriter
from .validator import RejectedMeasurement, ValidMeasurement, validate_event


def log_json(level: str, event: str, **fields: Any) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
        **fields,
    }
    print(json.dumps(record, ensure_ascii=False), flush=True)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _decode_message(msg: Message) -> tuple[ValidMeasurement | None, RejectedMeasurement | None]:
    raw_bytes = msg.value()
    if raw_bytes is None:
        return None, RejectedMeasurement(
            run_id="unknown",
            event_id=f"empty-{msg.partition()}-{msg.offset()}",
            station_id=None,
            determinand=None,
            reason="EMPTY_MESSAGE_VALUE",
            raw_payload={},
        )
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, RejectedMeasurement(
            run_id="unknown",
            event_id=f"raw-{msg.partition()}-{msg.offset()}",
            station_id=None,
            determinand=None,
            reason="INVALID_JSON",
            raw_payload={"error": str(exc), "raw_preview": raw_bytes[:200].decode("utf-8", "replace")},
        )

    result = validate_event(payload)
    return result.valid, result.rejected


def main() -> int:
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "water-quality-events")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "water-quality-consumer")
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        log_json("ERROR", "CONFIG_ERROR", message="DATABASE_URL is required")
        return 1

    consumer_mode = os.getenv("CONSUMER_MODE", "batch").strip().lower()
    if consumer_mode not in {"single", "batch"}:
        log_json("WARNING", "INVALID_CONSUMER_MODE", configured=consumer_mode, fallback="batch")
        consumer_mode = "batch"

    configured_batch_size = max(1, int(os.getenv("CONSUMER_BATCH_SIZE", "100")))
    batch_size = 1 if consumer_mode == "single" else configured_batch_size
    batch_timeout_s = max(0.05, int(os.getenv("CONSUMER_BATCH_TIMEOUT_MS", "500")) / 1000.0)

    crash_after_db = _env_bool("CONSUMER_CRASH_AFTER_DB_COMMIT", False)
    crash_target_index = max(0, int(os.getenv("CONSUMER_CRASH_TARGET_INDEX", "0")))

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
        }
    )
    consumer.subscribe([topic])

    db_writer = DatabaseWriter(database_url=database_url)
    running = True

    def _handle_signal(signum: int, _frame: Any) -> None:
        nonlocal running
        log_json("INFO", "SHUTDOWN_SIGNAL_RECEIVED", signal=signum)
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log_json(
        "INFO",
        "CONSUMER_STARTED",
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        consumer_mode=consumer_mode,
        effective_batch_size=batch_size,
        batch_timeout_ms=int(batch_timeout_s * 1000),
        crash_after_db_commit=crash_after_db,
        crash_target_index=crash_target_index,
    )

    buffer: list[Message] = []
    batch_started_at: float | None = None
    total_events_processed = 0

    def flush_buffer() -> None:
        nonlocal buffer, batch_started_at, total_events_processed
        if not buffer:
            return

        valid_records: list[ValidMeasurement] = []
        rejected_records: list[RejectedMeasurement] = []
        for msg in buffer:
            valid, rejected = _decode_message(msg)
            if valid is not None:
                valid_records.append(valid)
            elif rejected is not None:
                rejected_records.append(rejected)

        t0 = time.monotonic()
        inserted_valid, deduped_valid, inserted_rejected = db_writer.write_batch(
            valid_records, rejected_records
        )
        db_ms = round((time.monotonic() - t0) * 1000.0, 2)

        sample_run_id = (
            valid_records[0].run_id
            if valid_records
            else (rejected_records[0].run_id if rejected_records else "unknown")
        )
        first_event_id = (
            valid_records[0].event_id
            if valid_records
            else (rejected_records[0].event_id if rejected_records else "unknown")
        )
        last_event_id = (
            valid_records[-1].event_id
            if valid_records
            else (rejected_records[-1].event_id if rejected_records else "unknown")
        )

        if deduped_valid > 0:
            log_json(
                "WARNING",
                "DEDUP_REPLAY",
                run_id=sample_run_id,
                deduped_count=deduped_valid,
                inserted_count=inserted_valid,
                first_event_id=first_event_id,
                last_event_id=last_event_id,
            )

        prev_total = total_events_processed
        total_events_processed += len(buffer)

        if crash_after_db and prev_total <= crash_target_index < total_events_processed:
            log_json(
                "CRITICAL",
                "INJECTED_CRASH_AFTER_DB_COMMIT",
                exit_code=77,
                run_id=sample_run_id,
                first_event_id=first_event_id,
                last_event_id=last_event_id,
                batch_events=len(buffer),
                inserted_valid=inserted_valid,
                offset_committed=False,
            )
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(77)

        consumer.commit(asynchronous=False)
        log_json(
            "INFO",
            "BATCH_COMMITTED",
            run_id=sample_run_id,
            batch_events=len(buffer),
            inserted_valid=inserted_valid,
            deduped_valid=deduped_valid,
            inserted_rejected=inserted_rejected,
            db_latency_ms=db_ms,
        )

        buffer = []
        batch_started_at = None

    try:
        while running:
            msg = consumer.poll(timeout=0.1)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log_json("ERROR", "KAFKA_POLL_ERROR", error=str(msg.error()))
                else:
                    if not buffer:
                        batch_started_at = time.monotonic()
                    buffer.append(msg)

            if buffer:
                elapsed = time.monotonic() - (batch_started_at or time.monotonic())
                if len(buffer) >= batch_size or elapsed >= batch_timeout_s:
                    flush_buffer()
    finally:
        if buffer:
            try:
                flush_buffer()
            except Exception as exc:
                log_json("ERROR", "FINAL_FLUSH_FAILED", error=str(exc))
        consumer.close()
        db_writer.close()
        log_json("INFO", "CONSUMER_STOPPED", total_events_processed=total_events_processed)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
