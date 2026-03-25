-- ============================================================
-- AdMute v4 — Initial Schema
-- Migration 001
-- ============================================================

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

-- ── SCHEMA VERSION TRACKER ───────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- ── ADS ──────────────────────────────────────────────────────
-- The fingerprinted ad vault. Each row is a known ad.
CREATE TABLE IF NOT EXISTS ads (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT,
    duration_seconds    REAL    NOT NULL,
    streaming_service   TEXT    DEFAULT 'Unknown',
    category            TEXT    DEFAULT 'Uncategorized',
    region              TEXT    DEFAULT 'CA',
    is_active           INTEGER DEFAULT 1,
    source              TEXT    DEFAULT 'mic',   -- 'mic' | 'upload'
    hash_count          INTEGER DEFAULT 0,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ── HASHES ───────────────────────────────────────────────────
-- Acoustic fingerprint hashes for each ad.
-- Covering index on (hash_value, ad_id) is the hot path.
CREATE TABLE IF NOT EXISTS hashes (
    hash_value  INTEGER NOT NULL,
    time_offset INTEGER NOT NULL,
    ad_id       INTEGER NOT NULL REFERENCES ads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_hashes_covering
    ON hashes (hash_value, ad_id);

CREATE INDEX IF NOT EXISTS idx_hashes_ad_id
    ON hashes (ad_id);

-- ── RECORDINGS ───────────────────────────────────────────────
-- Raw recordings captured via mic before fingerprinting.
-- Stays as 'pending_review' until user ingests it.
CREATE TABLE IF NOT EXISTS recordings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id               INTEGER REFERENCES ads(id) ON DELETE SET NULL,
    file_path           TEXT    NOT NULL,
    duration_seconds    REAL,
    status              TEXT    DEFAULT 'pending_review',
                        -- 'pending_review' | 'ingested' | 'rejected'
    streaming_service   TEXT    DEFAULT 'Unknown',
    category            TEXT    DEFAULT 'Uncategorized',
    notes               TEXT,
    recorded_at         DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ── MUTE LOG ─────────────────────────────────────────────────
-- Every mute event, successful or otherwise.
CREATE TABLE IF NOT EXISTS mute_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id               INTEGER REFERENCES ads(id) ON DELETE SET NULL,
    ad_name_snapshot    TEXT,   -- denormalised: survives ad deletion
    muted_at            DATETIME DEFAULT CURRENT_TIMESTAMP,
    unmuted_at          DATETIME,
    duration_actual     REAL,
    mute_method         TEXT    DEFAULT 'ir',  -- 'ir' | 'cec'
    confidence_score    INTEGER,
    was_false_positive  INTEGER DEFAULT 0
);

-- ── SNR LOG ──────────────────────────────────────────────────
-- Acoustic placement quality measurements.
CREATE TABLE IF NOT EXISTS snr_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    measured_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    snr_db          REAL,
    classification  TEXT,   -- 'pass' | 'warn' | 'fail'
    placement_note  TEXT
);

-- ── DEVICE STATE ─────────────────────────────────────────────
-- Persistent key-value store for daemon state and config.
CREATE TABLE IF NOT EXISTS device_state (
    key     TEXT PRIMARY KEY,
    value   TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Seed defaults
INSERT OR IGNORE INTO device_state (key, value) VALUES
    ('mute_backend',        'ir'),
    ('mic_active',          '1'),
    ('ir_remote_name',      'hisense'),
    ('ir_key_mute',         'KEY_MUTE'),
    ('confidence_threshold','150'),
    ('safety_margin_secs',  '1.0'),
    ('cooldown_secs',       '30'),
    ('last_snr_db',         NULL),
    ('last_snr_class',      NULL);
