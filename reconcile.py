"""
AdMute v4 — reconcile.py
Checks and repairs consistency between the SQLite database
and the recordings directory on disk.

Usage:
    python reconcile.py          ← dry run, reports issues only
    python reconcile.py --fix    ← actually fixes all issues found
    python reconcile.py --fix --yes  ← no confirmation prompt
"""

import os
import sys
import argparse
import sqlite3
import logging
from pathlib import Path

BASE = Path(__file__).parent.resolve()
sys.path.insert(0, str(BASE))

from db import get_conn, DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)
log = logging.getLogger("reconcile")

# ── ANSI colours ─────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):    log.info(f"  {GREEN}✔{RESET}  {msg}")
def warn(msg):  log.info(f"  {YELLOW}⚠{RESET}  {msg}")
def bad(msg):   log.info(f"  {RED}✘{RESET}  {msg}")
def info(msg):  log.info(f"  {DIM}·{RESET}  {msg}")
def fixed(msg): log.info(f"  {CYAN}→{RESET}  {msg}")


def run_reconcile(fix: bool = False, yes: bool = False) -> None:
    print()
    print(f"{BOLD}AdMute v4 — Database Reconciler{RESET}")
    print(f"{'DRY RUN — no changes will be made' if not fix else 'FIX MODE — issues will be repaired'}")
    print(f"DB: {DB_PATH}")
    print("─" * 50)
    print()

    issues = []
    fixes  = []

    with get_conn() as conn:

        # ── CHECK 1: WAV files on disk with no recordings row ─────
        print(f"{BOLD}[1] Orphan WAV files{RESET} (files on disk, no DB row)")

        rec_dir = BASE / "recordings"
        if rec_dir.exists():
            wav_files = list(rec_dir.glob("*.wav"))
            known_paths = {
                row[0] for row in
                conn.execute("SELECT file_path FROM recordings").fetchall()
            }
            orphan_wavs = [f for f in wav_files if str(f) not in known_paths]

            if not orphan_wavs:
                ok(f"No orphan WAV files found ({len(wav_files)} files checked)")
            else:
                for f in orphan_wavs:
                    size_kb = f.stat().st_size // 1024
                    bad(f"Orphan WAV: {f.name} ({size_kb}KB)")
                    issues.append(f"orphan_wav:{f}")
                    if fix:
                        fixes.append(("delete_file", f))
        else:
            info("recordings/ directory not found — skipping")

        print()

        # ── CHECK 2: recordings rows pointing to missing WAV files ─
        print(f"{BOLD}[2] Dead recording rows{RESET} (DB row, missing WAV file)")

        rows = conn.execute(
            "SELECT id, file_path, status FROM recordings"
        ).fetchall()

        dead_rows = [r for r in rows if not Path(r[1]).exists()]

        if not dead_rows:
            ok(f"All {len(rows)} recording rows have valid files")
        else:
            for r in dead_rows:
                bad(f"Missing file: recordings.id={r[0]}  status={r[2]}  path={Path(r[1]).name}")
                issues.append(f"dead_row:{r[0]}")
                if fix:
                    fixes.append(("mark_rejected", r[0]))

        print()

        # ── CHECK 3: Ads with no hashes ───────────────────────────
        print(f"{BOLD}[3] Ghost ads{RESET} (ads table entries with zero hashes)")

        ghost_ads = conn.execute(
            """SELECT a.id, a.name, a.is_active
               FROM ads a
               LEFT JOIN hashes h ON h.ad_id = a.id
               WHERE h.ad_id IS NULL"""
        ).fetchall()

        if not ghost_ads:
            ok("No ghost ads found")
        else:
            for a in ghost_ads:
                state = "active" if a[2] else "inactive"
                bad(f"Ghost ad: id={a[0]}  name=\"{a[1]}\"  ({state}, 0 hashes)")
                issues.append(f"ghost_ad:{a[0]}")
                if fix:
                    fixes.append(("deactivate_ad", a[0]))

        print()

        # ── CHECK 4: Hashes pointing to non-existent ad_id ────────
        print(f"{BOLD}[4] Orphan hashes{RESET} (hash rows referencing deleted ads)")

        orphan_hashes = conn.execute(
            """SELECT h.ad_id, COUNT(*) as cnt
               FROM hashes h
               LEFT JOIN ads a ON a.id = h.ad_id
               WHERE a.id IS NULL
               GROUP BY h.ad_id"""
        ).fetchall()

        if not orphan_hashes:
            ok("No orphan hash rows found")
        else:
            for row in orphan_hashes:
                bad(f"Orphan hashes: ad_id={row[0]} has {row[1]:,} hash rows with no parent ad")
                issues.append(f"orphan_hashes:{row[0]}")
                if fix:
                    fixes.append(("delete_orphan_hashes", row[0]))

        print()

        # ── CHECK 5: Ingested recordings with no ad_id ────────────
        print(f"{BOLD}[5] Broken ingest links{RESET} (ingested recordings missing ad_id)")

        broken = conn.execute(
            """SELECT id, file_path FROM recordings
               WHERE status = 'ingested' AND ad_id IS NULL"""
        ).fetchall()

        if not broken:
            ok("All ingested recordings have valid ad_id links")
        else:
            for r in broken:
                bad(f"Broken link: recordings.id={r[0]} is ingested but has no ad_id")
                issues.append(f"broken_ingest:{r[0]}")
                if fix:
                    fixes.append(("mark_rejected", r[0]))

        print()

        # ── CHECK 6: Ads with no recording row ───────────────────
        print(f"{BOLD}[6] Unlinked ads{RESET} (vault entries with no source recording)")

        unlinked = conn.execute(
            """SELECT a.id, a.name, a.hash_count
               FROM ads a
               LEFT JOIN recordings r ON r.ad_id = a.id
               WHERE r.id IS NULL"""
        ).fetchall()

        if not unlinked:
            ok("All vault ads have a linked recording row")
        else:
            for a in unlinked:
                bad(f"Unlinked ad: id={a[0]}  name=\"{a[1]}\"  hashes={a[2]}")
                issues.append(f"unlinked_ad:{a[0]}")
                if fix:
                    fixes.append(("deactivate_ad", a[0]))

        # ── SUMMARY ───────────────────────────────────────────────
        print("─" * 50)
        if not issues:
            print(f"\n{GREEN}{BOLD}✔ Everything looks clean. No issues found.{RESET}\n")
            return

        print(f"\n{YELLOW}{BOLD}Found {len(issues)} issue(s).{RESET}\n")

        if not fix:
            print(f"  Run {CYAN}python reconcile.py --fix{RESET} to repair all issues.")
            print()
            return

        # ── APPLY FIXES ───────────────────────────────────────────
        if not yes:
            response = input(f"  Apply {len(fixes)} fix(es)? [y/N] ").strip().lower()
            if response != 'y':
                print("  Aborted.")
                print()
                return

        print()
        applied = 0

        for action, target in fixes:

            if action == "delete_file":
                try:
                    Path(target).unlink()
                    fixed(f"Deleted orphan file: {Path(target).name}")
                    applied += 1
                except Exception as e:
                    bad(f"Could not delete {target}: {e}")

            elif action == "mark_rejected":
                try:
                    conn.execute(
                        "UPDATE recordings SET status='rejected' WHERE id=?",
                        (target,)
                    )
                    fixed(f"Marked recordings.id={target} as rejected")
                    applied += 1
                except Exception as e:
                    bad(f"Could not update row {target}: {e}")

            elif action == "deactivate_ad":
                try:
                    conn.execute(
                        "UPDATE ads SET is_active=0 WHERE id=?",
                        (target,)
                    )
                    fixed(f"Deactivated ghost ad id={target}")
                    applied += 1
                except Exception as e:
                    bad(f"Could not deactivate ad {target}: {e}")

            elif action == "delete_orphan_hashes":
                try:
                    n = conn.execute(
                        "SELECT COUNT(*) FROM hashes WHERE ad_id=?",
                        (target,)
                    ).fetchone()[0]
                    conn.execute(
                        "DELETE FROM hashes WHERE ad_id=?", (target,)
                    )
                    fixed(f"Deleted {n:,} orphan hashes for ad_id={target}")
                    applied += 1
                except Exception as e:
                    bad(f"Could not delete hashes for ad_id {target}: {e}")

        conn.commit()

        print()
        print(f"{GREEN}{BOLD}✔ Applied {applied}/{len(fixes)} fixes.{RESET}")
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AdMute database reconciler"
    )
    parser.add_argument(
        "--fix", action="store_true",
        help="Apply fixes (default is dry run)"
    )
    parser.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip confirmation prompt"
    )
    args = parser.parse_args()
    run_reconcile(fix=args.fix, yes=args.yes)
