"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Process 3 — MatchEngine (Tiered Cache Edition)

Receives fingerprint hash batches from FingerprintWorkers and scores
them against a three-tier cache:

  L1 (Kings)   — top-5 ads by 30-day detection count, resident in RAM
  L2 (Context) — last-50 detected ads in an OrderedDict RAM cache
  L3 (Vault)   — SQLite fallback; hits are warmed into L2 automatically

Publishes MATCH, NEAR_MISS, and STRIKE events to MuteController and
the API via ZMQ PUB.  Cache is synced with the DB every 60 seconds.
"""

import os
import sys
import time
import json
import signal
import sqlite3
import threading
import numpy as np
import zmq
from collections  import Counter, defaultdict, OrderedDict
from dataclasses  import dataclass
from typing       import Optional, Iterable

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config  import load_config
from db      import get_conn
from bus     import FINGER_PUSH, MATCH_PUB, TOPIC_MATCH, TOPIC_NEAR_MISS, TOPIC_STRIKE
from console import (setup_logging, _extra,
                     fmt_match, fmt_near_miss, fmt_strike,
                     fmt_no_candidates, fmt_vault_empty_periodic)

PROC = "MATCH"


# ── AD ENTRY ──────────────────────────────────────────────────────────────────

@dataclass
class AdEntry:
    """
    One ad's full identity and hash map, resident in RAM.

    hashes maps hash_value → time_offset — identical to the data in the
    SQLite hashes table, loaded once so time-coherence scoring requires
    zero database I/O for L1 and L2 tier hits.
    """
    ad_id:    int
    ad_name:  str
    duration: float
    hashes:   dict   # {hash_value (int): time_offset (int)}


# ── CACHE STATE ───────────────────────────────────────────────────────────────
# Everything below is owned by the run() loop (single thread).
# _cache_lock uses RLock so that _heartbeat (called from the main loop)
# can safely call helper functions that also acquire the lock.

_cache_lock = threading.RLock()

# L1 — top-N by 30-day detection count
_l1: dict[int, AdEntry] = {}

# L2 — last-50-heard; tail = most recent, head = oldest (first to evict)
_l2: OrderedDict = OrderedDict()


# ── SCORING ───────────────────────────────────────────────────────────────────

def _score_entry(query_map: dict, entry: AdEntry) -> tuple:
    """
    Time-coherence score for one AdEntry against the live query map.

    Returns (score, peak_delta) where:
      score      — height of the tallest delta bin (match confidence)
      peak_delta — the modal bin value (STFT frames from start of ad)

    peak_delta is used by mute_controller to calculate how far into the
    ad we are at detection time, so the unmute timer fires at the right
    moment even when the mute fires mid-ad.
    """
    deltas = []
    for h_val, q_off in query_map.items():
        db_off = entry.hashes.get(h_val)
        if db_off is not None:
            deltas.append(db_off - q_off)
    if not deltas:
        return 0, 0
    c = Counter(deltas)
    peak_delta, peak_count = c.most_common(1)[0]
    return peak_count, peak_delta


def _score_tier(
    query_map: dict,
    entries:   Iterable[AdEntry],
) -> Optional[tuple]:
    """
    Score every AdEntry in a tier.
    Returns (best_score, best_entry, best_peak_delta) or None if empty.
    Caller must hold _cache_lock.
    """
    best_score      = 0
    best_entry      = None
    best_peak_delta = 0
    for entry in entries:
        score, peak_delta = _score_entry(query_map, entry)
        if score > best_score:
            best_score      = score
            best_entry      = entry
            best_peak_delta = peak_delta
    return (best_score, best_entry, best_peak_delta) if best_entry is not None else None


def _score_l3(query_map: dict, db_rows: list) -> dict:
    """
    Time-coherence scoring for L3 SQLite rows.
    Returns {ad_id: (score, ad_name, duration_seconds, peak_delta)}.
    """
    ad_info   = {}
    ad_deltas = defaultdict(list)

    for row in db_rows:
        h_val = row["hash_value"]
        if h_val not in query_map:
            continue
        delta = row["time_offset"] - query_map[h_val]
        ad_deltas[row["ad_id"]].append(delta)
        ad_info[row["ad_id"]] = (row["name"], row["duration_seconds"])

    scores = {}
    for ad_id, deltas in ad_deltas.items():
        c          = Counter(deltas)
        peak_delta, peak_count = c.most_common(1)[0]
        name, dur  = ad_info[ad_id]
        scores[ad_id] = (peak_count, name, dur, peak_delta)

    return scores


# ── DATABASE HELPERS ──────────────────────────────────────────────────────────

def _load_entry(conn, ad_id: int, ad_name: str,
                duration: float) -> Optional[AdEntry]:
    """
    Load all hashes for one ad from SQLite into an AdEntry.
    Returns None if the ad has no hashes (should not happen in practice).
    """
    rows = conn.execute(
        "SELECT hash_value, time_offset FROM hashes WHERE ad_id = ?",
        (ad_id,)
    ).fetchall()
    if not rows:
        return None
    return AdEntry(
        ad_id    = ad_id,
        ad_name  = ad_name,
        duration = duration,
        hashes   = {r["hash_value"]: r["time_offset"] for r in rows},
    )


def _query_l3(conn, hash_values: list, exclude_ids: set) -> list:
    """
    SQLite hash lookup for the L3 fallback tier.

    Only returns rows for ads whose ad_id is NOT in exclude_ids
    (i.e. not already in L1 or L2 — no point re-checking them here).
    Uses the covering index idx_hashes_covering for performance.
    """
    if not hash_values:
        return []

    ph     = ",".join("?" * len(hash_values))
    params = list(hash_values)

    excl_clause = ""
    if exclude_ids:
        eph         = ",".join("?" * len(exclude_ids))
        excl_clause = f"AND h.ad_id NOT IN ({eph})"
        params     += list(exclude_ids)

    sql = f"""
        SELECT h.hash_value,
               h.time_offset,
               h.ad_id,
               a.name,
               a.duration_seconds
        FROM   hashes h
        JOIN   ads    a ON h.ad_id = a.id
        WHERE  a.is_active = 1
          AND  h.hash_value IN ({ph})
          {excl_clause}
    """
    return conn.execute(sql, params).fetchall()


def _vault_size(conn) -> int:
    row = conn.execute("SELECT COUNT(*) FROM ads WHERE is_active = 1").fetchone()
    return row[0] if row else 0


# ── CACHE MUTATIONS ───────────────────────────────────────────────────────────
# These helpers assume _cache_lock is already held by the caller.

def _l2_insert_locked(entry: AdEntry, l2_max: int) -> None:
    """
    Insert or promote an entry to L2 tail (most recent slot).

    Rules (enforced in order):
      • If ad_id is in L1 → skip (no cross-tier duplicates).
      • If ad_id is already in L2 → promote to tail (O(1) move_to_end).
      • Otherwise → add to tail, then evict from head until len ≤ l2_max.

    Assumes _cache_lock is held.
    """
    if entry.ad_id in _l1:
        return
    if entry.ad_id in _l2:
        _l2.move_to_end(entry.ad_id)
        return
    _l2[entry.ad_id] = entry
    _l2.move_to_end(entry.ad_id)
    while len(_l2) > l2_max:
        _l2.popitem(last=False)          # evict oldest from head


# ── HEARTBEAT ─────────────────────────────────────────────────────────────────

def _heartbeat(cfg, log) -> None:
    """
    Maintenance cycle — called from the main match loop every heartbeat_seconds.

    Holds _cache_lock for the full duration so an in-flight match never
    reads a partially-updated cache.  The typical execution time when nothing
    has changed is < 5 ms (two tiny SQL queries + set arithmetic).
    Hash loading only happens when ads are newly added, promoted, or demoted.
    """
    l1_max      = cfg.cache.l1_size
    l2_max      = cfg.cache.l2_size
    window_days = cfg.cache.l1_window_days

    with _cache_lock:
        try:
            with get_conn() as conn:

                # ── 1. Sync active ad set ──────────────────────────────────
                active_rows = conn.execute(
                    "SELECT id, name, duration_seconds FROM ads WHERE is_active = 1"
                ).fetchall()
                active_ids  = {r["id"] for r in active_rows}
                active_map  = {r["id"]: r for r in active_rows}  # id → Row

                dead_ids = (set(_l1) | set(_l2)) - active_ids
                for ad_id in dead_ids:
                    _l1.pop(ad_id, None)
                    _l2.pop(ad_id, None)
                    log.info(
                        "Cache: evicted ad_id=%d (deactivated or purged)", ad_id,
                        extra=_extra(PROC)
                    )

                # ── 2. Load newly ingested ads → L2 tail ──────────────────
                cached_ids = set(_l1) | set(_l2)
                new_ids    = active_ids - cached_ids
                for ad_id in new_ids:
                    r = active_map[ad_id]
                    entry = _load_entry(conn, ad_id, r["name"], r["duration_seconds"])
                    if entry:
                        _l2_insert_locked(entry, l2_max)
                        log.info(
                            "Cache: '%s' (id=%d) → L2 tail (new ingest)",
                            r["name"], ad_id, extra=_extra(PROC)
                        )

                # ── 3. Recalculate L1 from 30-day mute_log stats ──────────
                top_rows = conn.execute("""
                    SELECT   m.ad_id,
                             COUNT(*) AS cnt
                    FROM     mute_log  m
                    JOIN     ads       a ON m.ad_id = a.id
                    WHERE    m.was_false_positive = 0
                      AND    a.is_active          = 1
                      AND    m.muted_at >= datetime('now', ?)
                    GROUP BY m.ad_id
                    ORDER BY cnt DESC
                    LIMIT    ?
                """, (f"-{window_days} days", l1_max)).fetchall()

                new_l1_ids = {r["ad_id"] for r in top_rows}
                old_l1_ids = set(_l1)

                # ── 4. Demote ads falling out of L1 → L2 tail ─────────────
                for ad_id in old_l1_ids - new_l1_ids:
                    entry = _l1.pop(ad_id)
                    _l2_insert_locked(entry, l2_max)
                    log.info(
                        "Cache: '%s' demoted L1 → L2 tail",
                        entry.ad_name, extra=_extra(PROC)
                    )

                # ── 5. Promote ads entering L1 ─────────────────────────────
                for ad_id in new_l1_ids - old_l1_ids:
                    # Prefer pulling the already-loaded entry from L2;
                    # fall back to a DB load if the ad wasn't cached.
                    entry = _l2.pop(ad_id, None)
                    if entry is None:
                        r = active_map.get(ad_id)
                        if r:
                            entry = _load_entry(
                                conn, ad_id, r["name"], r["duration_seconds"]
                            )
                    if entry:
                        _l1[ad_id] = entry
                        log.info(
                            "Cache: '%s' promoted → L1",
                            entry.ad_name, extra=_extra(PROC)
                        )

                # ── 6. Enforce L2 size cap ─────────────────────────────────
                while len(_l2) > l2_max:
                    evicted_id, evicted = _l2.popitem(last=False)
                    log.debug(
                        "Cache: L2 overflow — evicted '%s' (id=%d)",
                        evicted.ad_name, evicted_id, extra=_extra(PROC)
                    )

                # ── 7. Hard invariant check: L1 ∩ L2 = ∅ ──────────────────
                overlap = set(_l1) & set(_l2)
                for ad_id in overlap:
                    _l2.pop(ad_id, None)
                    log.warning(
                        "Cache: invariant violation fixed — ad_id=%d was in "
                        "both L1 and L2 simultaneously", ad_id,
                        extra=_extra(PROC)
                    )

                log.info(
                    "♥ Cache heartbeat — L1=%d kings | L2=%d context | "
                    "L3=SQLite | vault=%d active ads",
                    len(_l1), len(_l2), len(active_ids),
                    extra=_extra(PROC)
                )

        except sqlite3.Error as exc:
            log.error("Heartbeat DB error: %s", exc, extra=_extra(PROC))


# ── PROCESS ENTRY POINT ───────────────────────────────────────────────────────

def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    cfg = load_config()
    log = setup_logging(PROC, log_level, log_dir)

    threshold     = cfg.match.confidence_threshold
    near_miss_min = int(threshold * cfg.match.near_miss_ratio)
    strike_min    = cfg.match.strike_min
    cooldown      = cfg.match.cooldown_seconds
    hb_interval   = cfg.cache.heartbeat_seconds
    l2_max        = cfg.cache.l2_size

    # ── ZMQ ──────────────────────────────────────────────────────────────────
    ctx      = zmq.Context()
    receiver = ctx.socket(zmq.PULL)
    receiver.set_hwm(16)
    receiver.bind(FINGER_PUSH)

    publisher = ctx.socket(zmq.PUB)
    publisher.bind(MATCH_PUB)
    time.sleep(0.2)   # let subscribers connect before first publish

    # ── Warm the cache before entering the hot loop ───────────────────────────
    log.info("Populating L1/L2 cache from vault…", extra=_extra(PROC))
    _heartbeat(cfg, log)

    # ── State ─────────────────────────────────────────────────────────────────
    cooldown_map:   dict[int, float] = {}
    queries_total   = 0
    matches_total   = 0
    last_heartbeat  = time.time()
    last_vault_warn = 0.0
    last_no_match   = 0.0

    # Pre-compute stft_hop once — used to convert peak_delta (frames)
    # to seconds so mute_controller can calculate the correct unmute time
    # even when detection fires mid-ad.
    stft_hop = int(cfg.fingerprint.nperseg * (1.0 - cfg.fingerprint.noverlap_ratio))

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    running = True

    def _stop(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    log.info(
        "Ready — threshold=%d near_miss≥%d strike≥%d "
        "cooldown=%.0fs heartbeat=%ds",
        threshold, near_miss_min, strike_min, cooldown, hb_interval,
        extra=_extra(PROC)
    )

    while running:
        try:
            now = time.time()

            # ── Heartbeat ─────────────────────────────────────────────────────
            if now - last_heartbeat >= hb_interval:
                _heartbeat(cfg, log)
                last_heartbeat = time.time()

            # ── Poll for fingerprint batch (500 ms timeout) ───────────────────
            if not receiver.poll(timeout=500):
                continue

            parts = receiver.recv_multipart()
            if len(parts) != 2:
                continue

            meta_bytes, hashes_bytes = parts
            meta = json.loads(meta_bytes)

            flat = np.frombuffer(hashes_bytes, dtype=np.int32)
            if len(flat) < 2:
                continue

            pairs     = flat.reshape(-1, 2)
            query_map = {int(h): int(off) for h, off in pairs}
            hash_list = list(query_map.keys())
            queries_total += 1

            # ── Phase A: RAM tiers ────────────────────────────────────────────
            # Acquire lock briefly for the full L1+L2 scoring pass.
            # No I/O happens here — pure dict lookups and Counter arithmetic.
            best_score      = 0
            best_entry      = None
            best_peak_delta = 0
            match_tier      = None
            exclude_ids     = set()

            with _cache_lock:
                # L1
                r = _score_tier(query_map, _l1.values())
                if r and r[0] > best_score:
                    best_score, best_entry, best_peak_delta = r
                    match_tier = "L1"

                # L2 — only enter if L1 did not produce a confident match
                if best_score < threshold:
                    r = _score_tier(query_map, _l2.values())
                    if r and r[0] > best_score:
                        best_score, best_entry, best_peak_delta = r
                        match_tier = "L2"

                # Snapshot the excluded set for the L3 query (done outside lock)
                exclude_ids = set(_l1) | set(_l2)

            # ── Phase B: L3 SQLite fallback ───────────────────────────────────
            # Only reached if neither L1 nor L2 returned a confident match.
            query_ms = 0.0
            if best_score < threshold:
                t_q = time.perf_counter()
                try:
                    with get_conn() as conn:
                        vault_sz = _vault_size(conn)

                        if vault_sz == 0 and not exclude_ids:
                            # Entire vault is empty — warn periodically
                            now2 = time.time()
                            if now2 - last_vault_warn >= 120.0:
                                log.warning(
                                    fmt_vault_empty_periodic(),
                                    extra=_extra(PROC)
                                )
                                last_vault_warn = now2
                        else:
                            db_rows = _query_l3(conn, hash_list, exclude_ids)
                            if db_rows:
                                l3_scores = _score_l3(query_map, db_rows)
                                if l3_scores:
                                    top_id, (top_sc, top_name, top_dur, top_pd) = max(
                                        l3_scores.items(),
                                        key=lambda kv: kv[1][0]
                                    )
                                    if top_sc > best_score:
                                        best_score      = top_sc
                                        best_peak_delta = top_pd
                                        best_entry      = AdEntry(
                                            ad_id    = top_id,
                                            ad_name  = top_name,
                                            duration = top_dur,
                                            hashes   = {},
                                        )
                                        match_tier = "L3"

                except sqlite3.Error as exc:
                    log.error("L3 query error: %s", exc, extra=_extra(PROC))
                    time.sleep(0.5)

                query_ms = (time.perf_counter() - t_q) * 1000

            # ── No candidate at all ───────────────────────────────────────────
            if best_entry is None:
                now2 = time.time()
                if now2 - last_no_match >= 10.0:
                    log.debug(fmt_no_candidates(len(exclude_ids)),
                              extra=_extra(PROC))
                    last_no_match = now2
                continue

            now = time.time()

            # ── MATCH — confident hit ─────────────────────────────────────────
            if best_score >= threshold:
                last_t = cooldown_map.get(best_entry.ad_id, 0.0)
                if now - last_t < cooldown:
                    log.debug(
                        "Suppressed (cooldown) — \"%s\" %.0fs remaining",
                        best_entry.ad_name, cooldown - (now - last_t),
                        extra=_extra(PROC)
                    )
                    continue

                cooldown_map[best_entry.ad_id] = now
                matches_total += 1

                # Convert peak_delta (STFT frames) to seconds into the ad.
                # If detection fired 10s in, mute_controller subtracts this
                # from duration so the unmute timer fires at the right moment.
                time_offset_secs = round(
                    best_peak_delta * stft_hop / cfg.audio.sample_rate, 2
                )

                log.info(
                    fmt_match(best_entry.ad_name, best_score,
                              threshold, best_entry.duration)
                    + f"  [{match_tier}]",
                    extra=_extra(PROC)
                )

                publisher.send_multipart([
                    TOPIC_MATCH,
                    json.dumps({
                        "ad_id":            best_entry.ad_id,
                        "ad_name":          best_entry.ad_name,
                        "duration":         best_entry.duration,
                        "score":            best_score,
                        "time_offset_secs": time_offset_secs,
                        "query_ms":         round(query_ms, 1),
                        "tier":             match_tier,
                        "ts":               now,
                    }).encode(),
                ])

                # L3 hit → load full hashes and warm into L2
                if match_tier == "L3":
                    try:
                        with get_conn() as conn:
                            full = _load_entry(
                                conn,
                                best_entry.ad_id,
                                best_entry.ad_name,
                                best_entry.duration,
                            )
                        if full:
                            with _cache_lock:
                                _l2_insert_locked(full, l2_max)
                            log.debug(
                                "Cache: L3 hit '%s' → warmed into L2 tail",
                                full.ad_name, extra=_extra(PROC)
                            )
                    except sqlite3.Error as exc:
                        log.error("L2 warm-up error: %s", exc, extra=_extra(PROC))

                # L2 hit → promote to tail (most recent)
                elif match_tier == "L2":
                    with _cache_lock:
                        if best_entry.ad_id in _l2:
                            _l2.move_to_end(best_entry.ad_id)

            # ── NEAR MISS ─────────────────────────────────────────────────────
            # Only surface at INFO when ≥60% of threshold — lower readings
            # are scattered noise collisions with no operational value.
            # All near misses still publish via ZMQ for the frontend.
            elif best_score >= near_miss_min:
                if best_score >= int(threshold * 0.6):
                    log.info(
                        fmt_near_miss(best_entry.ad_name, best_score, threshold),
                        extra=_extra(PROC)
                    )
                else:
                    log.debug(
                        fmt_near_miss(best_entry.ad_name, best_score, threshold),
                        extra=_extra(PROC)
                    )
                publisher.send_multipart([
                    TOPIC_NEAR_MISS,
                    json.dumps({
                        "ad_id":   best_entry.ad_id,
                        "ad_name": best_entry.ad_name,
                        "score":   best_score,
                        "tier":    match_tier,
                        "ts":      now,
                    }).encode(),
                ])

            # ── STRIKE ────────────────────────────────────────────────────────
            # Strikes are single hash collisions — DEBUG only in production.
            # Still published via ZMQ so the frontend can display them.
            elif best_score >= strike_min:
                log.debug(
                    fmt_strike(best_entry.ad_name, best_score, threshold),
                    extra=_extra(PROC)
                )
                publisher.send_multipart([
                    TOPIC_STRIKE,
                    json.dumps({
                        "ad_id":   best_entry.ad_id,
                        "ad_name": best_entry.ad_name,
                        "score":   best_score,
                        "tier":    match_tier,
                        "ts":      now,
                    }).encode(),
                ])

        except zmq.ZMQError as exc:
            if running:
                log.error("ZMQ error: %s", exc, extra=_extra(PROC))
            break
        except Exception as exc:
            log.error("Unexpected error: %s", exc, extra=_extra(PROC),
                      exc_info=True)
            time.sleep(0.1)

    receiver.close()
    publisher.close()
    ctx.term()
    log.info("Stopped cleanly — %d total matches", matches_total,
             extra=_extra(PROC))


if __name__ == "__main__":
    run()