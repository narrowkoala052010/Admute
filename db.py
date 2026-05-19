"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Database manager — migration runner, connection factory, and
device_state key-value helpers.  All processes share this module.
"""


import os
import sys
import sqlite3
import logging
from pathlib import Path

log = logging.getLogger("admute.db")

BASE_DIR   = Path(__file__).parent.resolve()
DB_PATH    = BASE_DIR / "admute.db"
MIGRATIONS = BASE_DIR / "migrations"


# ── CONNECTION FACTORY ────────────────────────────────────────────────────────

def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Return a new SQLite connection with recommended PRAGMAs applied.

    Each call returns an independent connection — callers are responsible
    for closing it.  Use as a context manager wherever possible:

        with get_conn() as conn:
            conn.execute(...)

    PRAGMAs applied:
      WAL mode        — concurrent readers never block the writer
      NORMAL sync     — safe on power loss for WAL; no fsync on every write
      foreign_keys ON — enforce referential integrity
      cache_size 8MB  — reduces redundant I/O on the Pi
      temp_store MEM  — sort/group scratch space stays in RAM
    """
    conn = sqlite3.connect(db_path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous  = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA cache_size   = -8000")   # 8 MB page cache
    conn.execute("PRAGMA temp_store   = MEMORY")
    return conn


# ── MIGRATION RUNNER ──────────────────────────────────────────────────────────

def _get_applied_versions(conn: sqlite3.Connection) -> set:
    """Return the set of migration version numbers already applied."""
    try:
        rows = conn.execute("SELECT version FROM schema_version").fetchall()
        return {r["version"] for r in rows}
    except sqlite3.OperationalError:
        # schema_version table doesn't exist yet — fresh DB
        return set()


def _parse_version(filename: str) -> int:
    """Extract the numeric prefix from a migration filename like 001_name.sql."""
    try:
        return int(filename.split("_")[0])
    except (ValueError, IndexError):
        raise ValueError(
            f"Migration filename must start with NNN_: {filename}"
        )


def run_migrations(db_path: Path = DB_PATH) -> None:
    """
    Discover and apply all pending SQL migrations in the migrations/ folder.

    Files must be named NNN_description.sql where NNN is a zero-padded
    integer.  Migrations are applied in ascending numeric order.

    Each migration file manages its own transaction (BEGIN / COMMIT).
    If a migration fails, the error is logged and the process exits — a
    half-applied schema is worse than no schema.
    """
    if not MIGRATIONS.exists():
        log.error("Migrations directory not found: %s", MIGRATIONS)
        sys.exit(1)

    migration_files = sorted(MIGRATIONS.glob("*.sql"))
    if not migration_files:
        log.warning("No migration files found in %s", MIGRATIONS)
        return

    conn = get_conn(db_path)
    try:
        applied = _get_applied_versions(conn)

        pending = [
            f for f in migration_files
            if _parse_version(f.name) not in applied
        ]

        if not pending:
            log.info(
                "✔  Schema up to date (version %s)",
                max(applied, default=0)
            )
            return

        for mf in pending:
            version     = _parse_version(mf.name)
            description = mf.stem[4:].replace("_", " ")

            log.info("⟳  Applying migration %03d: %s", version, description)

            sql = mf.read_text(encoding="utf-8")
            try:
                # executescript issues an implicit COMMIT first, then runs the
                # script. Our migration files manage their own BEGIN/COMMIT, so
                # the transaction semantics are correct.
                conn.executescript(sql)
                log.info("✔  Migration %03d applied", version)
            except sqlite3.Error as exc:
                log.critical("✘  Migration %03d FAILED: %s", version, exc)
                sys.exit(1)

    finally:
        conn.close()


# ── SCHEMA VERIFICATION ───────────────────────────────────────────────────────

# The single source of truth for what the Hybrid schema contains.
# Any table, index, or column added via a future migration should be
# reflected here so verify_schema() catches regressions on startup.

EXPECTED_TABLES = {
    "schema_version",
    "ads",
    "hashes",
    "recordings",
    "mute_log",
    "snr_log",
    "device_state",
}

EXPECTED_INDEXES = {
    "idx_hashes_covering",    # L3 hot path — hash_value, ad_id
    "idx_hashes_ad_id",       # per-ad hash load / DELETE CASCADE
    "idx_mute_log_lookup",    # L1 heartbeat query — ad_id, muted_at, fp flag
}

EXPECTED_ADS_COLUMNS = {
    "id", "name", "duration_seconds", "streaming_service",
    "category", "region", "is_active", "source", "hash_count",
    "parent_ad_id", "created_at", "updated_at",
}


def verify_schema(db_path: Path = DB_PATH) -> bool:
    """
    Confirm the live DB matches the Hybrid schema.

    Returns True if everything checks out, False if anything is missing.
    Logs specific errors for every discrepancy so the operator knows
    exactly what to fix without reading source code.

    Called automatically by the CLI entry point below.  Processes can
    also call it at startup if they want an early abort on a bad DB.
    """
    conn = get_conn(db_path)
    ok   = True

    try:
        existing_tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        existing_indexes = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        existing_triggers = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }

        missing_tables  = EXPECTED_TABLES  - existing_tables
        missing_indexes = EXPECTED_INDEXES - existing_indexes

        if missing_tables:
            log.error("✘  Missing tables: %s", missing_tables)
            ok = False

        if missing_indexes:
            log.error("✘  Missing indexes: %s", missing_indexes)
            ok = False

        if "ads_updated_at" not in existing_triggers:
            log.error("✘  Missing trigger: ads_updated_at")
            ok = False

        # Verify ads columns — catch accidental schema drift
        if "ads" in existing_tables:
            actual_cols = {
                r["name"] for r in
                conn.execute("PRAGMA table_info(ads)").fetchall()
            }
            missing_cols = EXPECTED_ADS_COLUMNS - actual_cols
            if missing_cols:
                log.error("✘  Missing columns in ads: %s", missing_cols)
                ok = False

            # Dead v6 columns — warn if they somehow crept back in
            dead_cols = {
                "ultrasound_score", "infrasound_delta", "sigint_processed",
                "source_platform", "ad_category",
            }
            present_dead = dead_cols & actual_cols
            if present_dead:
                log.warning(
                    "⚠  Dead v6 columns still present in ads: %s — "
                    "consider running a cleanup migration",
                    present_dead
                )
                # Warning only — don't fail the check; they're harmless

        if ok:
            log.info("✔  Schema verified — AdMute Hybrid ready")

    finally:
        conn.close()

    return ok


# ── DEVICE STATE HELPERS ──────────────────────────────────────────────────────

def state_get(key: str, default=None, db_path: Path = DB_PATH):
    """Read a single value from device_state. Returns default if key absent."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM device_state WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default


def state_set(key: str, value, db_path: Path = DB_PATH) -> None:
    """
    Write a single value to device_state.
    Creates the key if absent, updates it if present.
    value is coerced to str; pass None to store a SQL NULL.
    """
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO device_state (key, value, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(key) DO UPDATE
               SET value      = excluded.value,
                   updated_at = excluded.updated_at""",
            (key, str(value) if value is not None else None)
        )
        conn.commit()


# ── CLI ENTRY POINT ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level   = logging.DEBUG,
        format  = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt = "%H:%M:%S",
    )

    print()
    print("═" * 55)
    print("  AdMute Hybrid — Database Initialisation")
    print(f"  DB      : {DB_PATH}")
    print(f"  Migrations: {MIGRATIONS}")
    print("═" * 55)
    print()

    run_migrations()
    print()
    verify_schema()

    print()
    print("─" * 55)
    print("  Device state:")
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT key, value FROM device_state ORDER BY key"
        ).fetchall()
        for row in rows:
            print(f"    {row['key']:<28} = {row['value']}")
    finally:
        conn.close()
    print("─" * 55)
    print()