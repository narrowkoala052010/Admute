"""
AdMute v6 — Database Manager
Handles initialisation, migrations, and provides a
thread-safe connection factory.
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
    for closing it (use as a context manager).
    """
    conn = sqlite3.connect(db_path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous  = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA cache_size   = -8000")   # 8MB page cache
    conn.execute("PRAGMA temp_store   = MEMORY")
    return conn


# ── MIGRATION RUNNER ──────────────────────────────────────────────────────────

def _get_applied_versions(conn: sqlite3.Connection) -> set[int]:
    """Return the set of migration version numbers already applied."""
    try:
        rows = conn.execute("SELECT version FROM schema_version").fetchall()
        return {r["version"] for r in rows}
    except sqlite3.OperationalError:
        # schema_version table doesn't exist yet — fresh DB
        return set()


def run_migrations(db_path: Path = DB_PATH) -> None:
    """
    Discover and apply all pending SQL migrations in the migrations/ folder.
    Files must be named NNN_description.sql where NNN is a zero-padded integer.
    Migrations are applied in ascending numeric order and are idempotent.
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
            log.info("✔  Database schema is up to date (version %s)",
                     max(applied, default=0))
            return

        for migration_file in pending:
            version     = _parse_version(migration_file.name)
            description = migration_file.stem[4:].replace("_", " ")

            log.info("⟳  Applying migration %03d: %s", version, description)

            sql = migration_file.read_text(encoding="utf-8")
            try:
                conn.executescript(sql)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_version (version, description) "
                    "VALUES (?, ?)",
                    (version, description)
                )
                conn.commit()
                log.info("✔  Migration %03d applied successfully", version)
            except sqlite3.Error as exc:
                conn.rollback()
                log.critical("✘  Migration %03d FAILED: %s", version, exc)
                sys.exit(1)

    finally:
        conn.close()


def _parse_version(filename: str) -> int:
    """Extract the numeric prefix from a migration filename like 001_name.sql."""
    try:
        return int(filename.split("_")[0])
    except (ValueError, IndexError):
        raise ValueError(f"Migration filename must start with NNN_: {filename}")


# ── SCHEMA VERIFICATION ───────────────────────────────────────────────────────

EXPECTED_TABLES = {
    "schema_version", "ads", "hashes", "recordings",
    "mute_log", "snr_log", "device_state", "markov_transitions"
}

EXPECTED_INDEXES = {
    "idx_hashes_covering", "idx_hashes_ad_id", "idx_markov_source"
}


def verify_schema(db_path: Path = DB_PATH) -> bool:
    """
    Verify all expected tables and indexes exist.
    Returns True if valid, False otherwise.
    """
    conn = get_conn(db_path)
    ok = True
    try:
        existing_tables = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        existing_indexes = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        }

        missing_tables = EXPECTED_TABLES - existing_tables
        missing_indexes = EXPECTED_INDEXES - existing_indexes

        if missing_tables:
            log.error("✘  Missing tables: %s", missing_tables)
            ok = False
        if missing_indexes:
            log.error("✘  Missing indexes: %s", missing_indexes)
            ok = False
        if ok:
            log.info("✔  Schema verification passed — all tables and indexes present")

    finally:
        conn.close()
    return ok


# ── DEVICE STATE HELPERS ──────────────────────────────────────────────────────

def state_get(key: str, default=None, db_path: Path = DB_PATH):
    """Read a single value from device_state."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM device_state WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default


def state_set(key: str, value, db_path: Path = DB_PATH) -> None:
    """Write a single value to device_state."""
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO device_state (key, value, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(key) DO UPDATE
               SET value = excluded.value,
                   updated_at = excluded.updated_at""",
            (key, str(value) if value is not None else None)
        )
        conn.commit()


# ── CLI ENTRY POINT ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S"
    )

    print()
    print("═" * 55)
    print("  AdMute v6 — Database Initialisation")
    print(f"  DB path : {DB_PATH}")
    print(f"  Migrations: {MIGRATIONS}")
    print("═" * 55)
    print()

    run_migrations()
    print()
    verify_schema()

    print()
    print("─" * 55)
    print("  Device state defaults:")
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