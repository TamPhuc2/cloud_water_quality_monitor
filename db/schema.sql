-- ============================================================
-- Water Quality Cloud Monitoring Schema (PostgreSQL 16 / Neon)
-- Supports Seminar MVP (idempotent streaming ingestion & alerts)
-- and Phase 2 BTL (ml_predictions hook table).
-- ============================================================

CREATE TABLE IF NOT EXISTS water_measurements (
    run_id           TEXT NOT NULL,
    event_id         TEXT NOT NULL,
    station_id       TEXT NOT NULL,
    station_name     TEXT NOT NULL,
    easting          DOUBLE PRECISION,
    northing         DOUBLE PRECISION,
    determinand      TEXT NOT NULL,
    determinand_code TEXT,
    result_qualifier TEXT,
    result_value     DOUBLE PRECISION NOT NULL,
    unit             TEXT,
    material_type    TEXT,
    is_compliance    BOOLEAN,
    alert_level      TEXT NOT NULL DEFAULT 'NORMAL',
    event_time       TIMESTAMPTZ NOT NULL,
    produced_at      TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_wm_ingested_at
    ON water_measurements (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_wm_run_det_event_time
    ON water_measurements (run_id, determinand, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_wm_station_det_event_time
    ON water_measurements (station_id, determinand, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_wm_run_alert_ingested
    ON water_measurements (run_id, alert_level, ingested_at DESC);


CREATE TABLE IF NOT EXISTS rejected_events (
    run_id       TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    station_id   TEXT,
    determinand  TEXT,
    reason       TEXT NOT NULL,
    raw_payload  JSONB,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_re_run_rejected_at
    ON rejected_events (run_id, rejected_at DESC);


CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    description     TEXT,
    consumer_mode   TEXT,
    batch_size      INTEGER,
    target_rate_eps DOUBLE PRECISION,
    planned_events  INTEGER,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at     TIMESTAMPTZ
);


-- Phase 2 (BTL) hook table for decoupled ML prediction & imputation worker
CREATE TABLE IF NOT EXISTS ml_predictions (
    run_id          TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    station_id      TEXT NOT NULL,
    determinand     TEXT NOT NULL,
    model_name      TEXT NOT NULL,
    predicted_value DOUBLE PRECISION NOT NULL,
    is_imputed      BOOLEAN NOT NULL DEFAULT FALSE,
    predicted_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, event_id, model_name)
);
