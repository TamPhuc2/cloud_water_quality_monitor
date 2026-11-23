"""Initialize Neon / PostgreSQL schema idempotently."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv


EXPECTED_TABLES = (
    "water_measurements",
    "rejected_events",
    "pipeline_runs",
    "ml_predictions",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize Water Quality PostgreSQL schema.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL connection string (overrides DATABASE_URL in environment/.env).",
    )
    parser.add_argument(
        "--schema",
        default=str(Path(__file__).resolve().with_name("schema.sql")),
        help="Path to schema.sql file.",
    )
    return parser.parse_args()


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    args = parse_args()
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL is not set. Pass --database-url or set it in .env.", file=sys.stderr)
        return 1

    schema_path = Path(args.schema)
    if not schema_path.is_file():
        print(f"ERROR: Schema file not found: {schema_path}", file=sys.stderr)
        return 1

    schema_sql = schema_path.read_text(encoding="utf-8")

    # prepare_threshold=None prevents DuplicatePreparedStatement errors with Neon PgBouncer pooler.
    with psycopg.connect(database_url, prepare_threshold=None, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            cur.execute(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public' AND tablename = ANY(%s)
                ORDER BY tablename
                """,
                (list(EXPECTED_TABLES),),
            )
            found = [row[0] for row in cur.fetchall()]
        conn.commit()

    missing = sorted(set(EXPECTED_TABLES) - set(found))
    print(f"Verified tables ({len(found)}/{len(EXPECTED_TABLES)}): {', '.join(found)}")
    if missing:
        print(f"ERROR: Missing expected tables: {', '.join(missing)}", file=sys.stderr)
        return 1

    print("Database initialization completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
