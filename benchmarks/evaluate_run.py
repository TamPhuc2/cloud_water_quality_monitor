"""Evaluate a Cloud benchmark run from Neon PostgreSQL, Producer JSON, and Kafka lag."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv


def compute_run_metrics(
    *,
    run_id: str,
    phase: str | None,
    db_stats: dict[str, Any],
    producer_report: dict[str, Any] | None = None,
    kafka_lag: int | None = None,
    expected_valid: int | None = None,
) -> dict[str, Any]:
    """Assemble standardized benchmark metrics dictionary from raw components."""
    valid_rows = int(db_stats.get("valid_rows", 0) or 0)
    distinct_valid = int(db_stats.get("distinct_valid", 0) or 0)
    rejected_rows = int(db_stats.get("rejected_rows", 0) or 0)
    total_ingested = valid_rows + rejected_rows
    duplicates = max(0, valid_rows - distinct_valid)

    drain_span_s = float(db_stats.get("drain_span_s", 0.0) or 0.0)
    if drain_span_s <= 0 and producer_report:
        drain_span_s = float(producer_report.get("elapsed_seconds", 0.0) or 0.0)
    db_throughput_eps = round(total_ingested / max(drain_span_s, 1e-3), 2) if total_ingested else 0.0

    result: dict[str, Any] = {
        "run_id": run_id,
        "phase": phase,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "valid_rows": valid_rows,
        "distinct_valid_events": distinct_valid,
        "duplicate_rows": duplicates,
        "rejected_rows": rejected_rows,
        "total_db_rows": total_ingested,
        "expected_valid": expected_valid,
        "db_drain_span_s": round(drain_span_s, 3),
        "db_throughput_eps": db_throughput_eps,
        "e2e_p50_ms": round(float(db_stats.get("p50_ms") or 0.0), 2),
        "e2e_p95_ms": round(float(db_stats.get("p95_ms") or 0.0), 2),
        "e2e_max_ms": round(float(db_stats.get("max_ms") or 0.0), 2),
        "alerts": db_stats.get("alerts", {}),
        "first_event_id": db_stats.get("first_event_id"),
        "kafka_lag": kafka_lag,
        "producer": producer_report,
    }
    return result


def query_db_stats(database_url: str, run_id: str) -> dict[str, Any]:
    """Query Neon / PostgreSQL for metrics of a single run_id."""
    with psycopg.connect(database_url, prepare_threshold=None, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::bigint AS valid_rows,
                    COUNT(DISTINCT event_id)::bigint AS distinct_valid,
                    EXTRACT(EPOCH FROM (MAX(ingested_at) - MIN(ingested_at))) AS drain_span_s,
                    percentile_cont(0.50) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (ingested_at - produced_at)) * 1000.0
                    ) AS p50_ms,
                    percentile_cont(0.95) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (ingested_at - produced_at)) * 1000.0
                    ) AS p95_ms,
                    MAX(EXTRACT(EPOCH FROM (ingested_at - produced_at)) * 1000.0) AS max_ms,
                    (ARRAY_AGG(event_id ORDER BY ingested_at ASC))[1] AS first_event_id
                FROM water_measurements
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()

            cur.execute(
                "SELECT COUNT(*)::bigint FROM rejected_events WHERE run_id = %s",
                (run_id,),
            )
            rejected_rows = cur.fetchone()[0]

            cur.execute(
                """
                SELECT alert_level, COUNT(*)::bigint
                FROM water_measurements
                WHERE run_id = %s
                GROUP BY alert_level
                """,
                (run_id,),
            )
            alerts = {lvl: int(cnt) for lvl, cnt in cur.fetchall()}

    return {
        "valid_rows": int(row[0] or 0),
        "distinct_valid": int(row[1] or 0),
        "drain_span_s": float(row[2] or 0.0),
        "p50_ms": float(row[3] or 0.0) if row[3] is not None else 0.0,
        "p95_ms": float(row[4] or 0.0) if row[4] is not None else 0.0,
        "max_ms": float(row[5] or 0.0) if row[5] is not None else 0.0,
        "first_event_id": row[6],
        "rejected_rows": int(rejected_rows or 0),
        "alerts": alerts,
    }


def query_kafka_lag(bootstrap: str, topic: str, group_id: str) -> int | None:
    """Calculate total consumer group lag across all partitions of topic."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id)])
        group_offsets = futures[group_id].result(timeout=10.0)
        committed_map = {
            (tp.topic, tp.partition): tp.offset
            for tp in group_offsets.topic_partitions
            if tp.topic == topic and tp.offset >= 0
        }

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"{group_id}-lag-probe",
                "enable.auto.commit": False,
            }
        )
        try:
            md = consumer.list_topics(topic=topic, timeout=10.0)
            topic_md = md.topics.get(topic)
            if topic_md is None:
                return None
            total_lag = 0
            for pid in topic_md.partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=5.0)
                committed = committed_map.get((topic, pid), low)
                total_lag += max(0, high - committed)
            return total_lag
        finally:
            consumer.close()
    except Exception as exc:
        print(f"WARNING: Could not query Kafka lag ({exc})", file=sys.stderr)
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate a Cloud benchmark run.")
    parser.add_argument("--run-id", required=True, help="Run ID to inspect.")
    parser.add_argument("--phase", default=None, help="Logical benchmark phase tag (e.g. burst-100).")
    parser.add_argument("--producer-report", default=None, help="Path to producer JSON report.")
    parser.add_argument("--expected-valid", type=int, default=None, help="Wait until this many valid rows arrive.")
    parser.add_argument("--wait-timeout", type=int, default=90, help="Max seconds to wait for expected rows.")
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers to query consumer lag.")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "water-quality-events"))
    parser.add_argument("--group", default=os.getenv("KAFKA_CONSUMER_GROUP", "water-quality-consumer"))
    parser.add_argument("--database-url", default=None, help="Override DATABASE_URL.")
    parser.add_argument("--output-json", default=None, help="Output path for metrics JSON.")
    return parser.parse_args()


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    args = parse_args()
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 1

    producer_report = None
    expected_valid = args.expected_valid
    expected_total = None
    if args.producer_report:
        report_path = Path(args.producer_report)
        if report_path.is_file():
            producer_report = json.loads(report_path.read_text(encoding="utf-8"))
            if expected_valid is None:
                expected_valid = producer_report.get("expected_valid_events")
            expected_total = producer_report.get("sent_events")

    deadline = time.monotonic() + args.wait_timeout
    db_stats = query_db_stats(database_url, args.run_id)
    while time.monotonic() < deadline:
        total_arrived = db_stats["valid_rows"] + db_stats["rejected_rows"]
        if expected_total is not None:
            if total_arrived >= expected_total:
                break
        elif expected_valid is not None:
            if db_stats["valid_rows"] >= expected_valid:
                break
        else:
            break
        time.sleep(1.5)
        db_stats = query_db_stats(database_url, args.run_id)

    kafka_lag = None
    if args.bootstrap:
        kafka_lag = query_kafka_lag(args.bootstrap, args.topic, args.group)

    metrics = compute_run_metrics(
        run_id=args.run_id,
        phase=args.phase,
        db_stats=db_stats,
        producer_report=producer_report,
        kafka_lag=kafka_lag,
        expected_valid=expected_valid,
    )

    out_path = Path(
        args.output_json
        or Path(__file__).resolve().parent / "results" / f"{args.run_id}.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
