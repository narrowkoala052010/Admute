-- ============================================================
-- AdMute Light — Initial Schema
-- Migration 001
--
-- This is the single authoritative schema for AdMute Light.
-- Every table, column, index, trigger, and seed value here
-- is actively used by the running system.  Nothing speculative,
-- nothing from abandoned v6 experiments.
--
-- Tiered cache (L1/L2/L3) lives entirely in match_engine.py RAM.
-- SQLite is L3 only — fast reads, clean writes, no dead weight.
-- ============================================================

PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA foreign_keys = ON;

-- ── SCHEMA VERSION TRACKER ───────────────────────────────────
-- run_migrations() in db.py checks this before applying any file.
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER  PRIMARY KEY,
    description TEXT,
    applied_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ── ADS — the fingerprinted vault ────────────────────────────
--
-- parent_ad_id: variant-family link.  A re-recorded version of an
--   existing ad points here so the UI can group them.  NULL = root.
--
-- updated_at: auto-maintained by the trigger below.  The MatchEngine
--   heartbeat queries this column to detect deactivations and renames
--   without scanning the full table.
--
-- source: 'mic' (recorded live) | 'upload' (future file import).
--
-- Columns deliberately NOT here:
--   ultrasound_score, infrasound_delta, sigint_processed,
--   source_platform, ad_category  — removed; never used in v4.
CREATE TABLE IF NOT EXISTS ads (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    name              TEXT,
    duration_seconds  REAL     NOT NULL,
    streaming_service TEXT     DEFAULT 'Unknown',
    category          TEXT     DEFAULT 'Uncategorized',
    region            TEXT     DEFAULT 'CA',
    is_active         INTEGER  DEFAULT 1,
    source            TEXT     DEFAULT 'mic',
    hash_count        INTEGER  DEFAULT 0,
    parent_ad_id      INTEGER  REFERENCES ads(id) ON DELETE SET NULL,
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Auto-refresh updated_at on every UPDATE.
-- Heartbeat in match_engine.py relies on this to detect changes.
CREATE TRIGGER IF NOT EXISTS ads_updated_at
AFTER UPDATE ON ads
BEGIN
    UPDATE ads
    SET    updated_at = CURRENT_TIMESTAMP
    WHERE  id = NEW.id;
END;

-- ── HASHES — acoustic fingerprint store ──────────────────────
--
-- idx_hashes_covering is the L3 hot path: a single index scan
-- returns (hash_value, ad_id) without touching the table data.
--
-- idx_hashes_ad_id supports fast DELETE CASCADE and per-ad
-- hash loading when warming entries into the L1/L2 RAM cache.
CREATE TABLE IF NOT EXISTS hashes (
    hash_value  INTEGER NOT NULL,
    time_offset INTEGER NOT NULL,
    ad_id       INTEGER NOT NULL REFERENCES ads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_hashes_covering
    ON hashes (hash_value, ad_id);

CREATE INDEX IF NOT EXISTS idx_hashes_ad_id
    ON hashes (ad_id);

-- ── RECORDINGS — raw captures before ingest ──────────────────
--
-- pending_link_ad_id: set by the ingest pipeline when the new
--   recording looks like a variant of an existing ad.  The Review
--   tab in the UI surfaces this as an accept/reject prompt.
CREATE TABLE IF NOT EXISTS recordings (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    ad_id             INTEGER  REFERENCES ads(id) ON DELETE SET NULL,
    file_path         TEXT     NOT NULL,
    duration_seconds  REAL,
    status            TEXT     DEFAULT 'pending_review',
                      -- 'pending_review' | 'ingested' | 'rejected'
    streaming_service TEXT     DEFAULT 'Unknown',
    category          TEXT     DEFAULT 'Uncategorized',
    notes             TEXT,
    pending_link_ad_id INTEGER REFERENCES ads(id) ON DELETE SET NULL,
    recorded_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ── MUTE LOG — every mute event ──────────────────────────────
--
-- ad_name_snapshot: denormalised copy of the ad name at mute time.
--   Survives ad deletion so history is never orphaned.
--
-- mute_method: 'cec' is the default and only supported backend
--   in AdMute Hybrid.  Column kept as TEXT for forward compatibility.
--
-- confidence_score: the time-coherence peak score that triggered
--   the mute.  Useful for tuning the threshold.
--
-- idx_mute_log_lookup covers the L1 heartbeat query:
--   WHERE was_false_positive = 0
--     AND muted_at >= datetime('now', '-30 days')
--   GROUP BY ad_id
-- A composite index on (ad_id, muted_at, was_false_positive) lets
-- SQLite satisfy this with an index-only scan.
CREATE TABLE IF NOT EXISTS mute_log (
    id                 INTEGER  PRIMARY KEY AUTOINCREMENT,
    ad_id              INTEGER  REFERENCES ads(id) ON DELETE SET NULL,
    ad_name_snapshot   TEXT,
    muted_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
    unmuted_at         DATETIME,
    duration_actual    REAL,
    mute_method        TEXT     DEFAULT 'cec',
    confidence_score   INTEGER,
    was_false_positive INTEGER  DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_mute_log_lookup
    ON mute_log (ad_id, muted_at, was_false_positive);

-- ── SNR LOG — microphone placement quality ───────────────────
CREATE TABLE IF NOT EXISTS snr_log (
    id             INTEGER  PRIMARY KEY AUTOINCREMENT,
    snr_db         REAL,
    classification TEXT,    -- 'pass' | 'warn' | 'fail'
    placement_note TEXT,
    measured_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ── DEVICE STATE — persistent daemon key-value store ─────────
--
-- Survives process restarts.  The API reads and writes these via
-- state_get() / state_set() in db.py.
-- config.toml is the source of truth for tunable parameters;
-- device_state holds runtime state (is_recording, last_snr, etc.)
-- and the generated api_secret.
CREATE TABLE IF NOT EXISTS device_state (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO device_state (key, value) VALUES
    ('mute_backend',    'cec'),
    ('mic_active',      '1'),
    ('is_recording',    '0'),
    ('last_snr_db',     NULL),
    ('last_snr_class',  NULL);

-- ── MARK THIS MIGRATION APPLIED ──────────────────────────────
INSERT OR IGNORE INTO schema_version (version, description)
VALUES (1, 'initial schema — AdMute Hybrid');