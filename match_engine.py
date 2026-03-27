"""
AdMute v4 — Process 3: MatchEngine
Pulls fingerprint hash lists from FingerprintWorker,
queries the SQLite vault, applies time-coherence scoring,
and publishes classified match events to MuteController.

This is the brain of AdMute. The feedback loop here is
what you see on the terminal: strikes, near misses, matches.
"""

import os
import sys
import time
import json
import signal
import sqlite3
import numpy as np
import zmq
from collections import Counter, defaultdict
from pathlib import Path

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config  import load_config
from db      import get_conn, DB_PATH
from bus     import FINGER_PUSH, MATCH_PUB, TOPIC_MATCH, TOPIC_NEAR_MISS, TOPIC_STRIKE
from console import (setup_logging, _extra,
                     fmt_match, fmt_near_miss, fmt_strike,
                     fmt_no_candidates, fmt_vault_empty_periodic)

PROC = "MATCH"


def _query_vault(conn: sqlite3.Connection,
                 hash_values: list[int]) -> list[sqlite3.Row]:
    """
    Find all hashes in the vault that match any of our query hashes.
    Returns rows of (hash_value, time_offset, ad_id, ad_name, duration_seconds).
    Uses the covering index idx_hashes_covering for performance.
    """
    if not hash_values:
        return []

    placeholders = ",".join("?" * len(hash_values))
    sql = f"""
        SELECT  h.hash_value,
                h.time_offset,
                h.ad_id,
                a.name,
                a.duration_seconds
        FROM    hashes h
        JOIN    ads a ON h.ad_id = a.id
        WHERE   a.is_active = 1
          AND   h.hash_value IN ({placeholders})
    """
    return conn.execute(sql, hash_values).fetchall()


def _time_coherence_score(
        query_map: dict[int, int],
        db_rows: list[sqlite3.Row],
) -> dict[int, tuple[int, str, float]]:
    """
    For each candidate ad, compute the time-coherence score.

    The score is the height of the tallest bin in a histogram of
    (db_offset - query_offset) values for matching hashes.
    A true match produces a sharp spike; noise produces a flat histogram.

    Returns: {ad_id: (score, ad_name, duration_seconds)}
    """
    ad_info:   dict[int, tuple[str, float]] = {}
    ad_deltas: dict[int, list[int]]         = defaultdict(list)

    for row in db_rows:
        h_val     = row["hash_value"]
        db_offset = row["time_offset"]
        ad_id     = row["ad_id"]

        if h_val not in query_map:
            continue

        delta = db_offset - query_map[h_val]
        ad_deltas[ad_id].append(delta)
        ad_info[ad_id] = (row["name"], row["duration_seconds"])

    scores: dict[int, tuple[int, str, float]] = {}
    for ad_id, deltas in ad_deltas.items():
        counter = Counter(deltas)
        peak    = counter.most_common(1)[0][1]
        name, dur = ad_info[ad_id]
        scores[ad_id] = (peak, name, dur)

    return scores


def _vault_size(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM ads WHERE is_active = 1"
    ).fetchone()
    return row[0] if row else 0


def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    cfg = load_config()
    log = setup_logging(PROC, log_level, log_dir)

    threshold      = cfg.match.confidence_threshold
    near_miss_min  = int(threshold * cfg.match.near_miss_ratio)
    strike_min     = cfg.match.strike_min
    cooldown       = cfg.match.cooldown_seconds

    # ── ZMQ ──────────────────────────────────────────────────
    ctx      = zmq.Context()
    receiver = ctx.socket(zmq.PULL)
    receiver.set_hwm(16)
    receiver.bind(FINGER_PUSH)

    publisher = ctx.socket(zmq.PUB)
    publisher.bind(MATCH_PUB)

    time.sleep(0.2)   # let subscribers connect

    # ── State ─────────────────────────────────────────────────
    cooldown_map: dict[int, float] = {}   # ad_id → last match time
    queries_total  = 0
    matches_total  = 0
    last_heartbeat = time.time()
    last_vault_warn= 0.0
    last_no_match  = 0.0    # rate-limit "no match" log spam

    # ── Graceful shutdown ─────────────────────────────────────
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    log.info(
        "Ready — threshold=%d  near_miss≥%d  strike≥%d  cooldown=%.0fs",
        threshold, near_miss_min, strike_min, cooldown,
        extra=_extra(PROC)
    )

    with get_conn() as conn:
        v = _vault_size(conn)
        if v == 0:
            log.warning(fmt_vault_empty_periodic(), extra=_extra(PROC))
        else:
            log.info("Vault loaded — %d active ads", v, extra=_extra(PROC))

    while running:
        try:
            if not receiver.poll(timeout=500):
                continue

            parts = receiver.recv_multipart()
            if len(parts) != 2:
                continue

            meta_bytes, hashes_bytes = parts
            meta = json.loads(meta_bytes)

            # Deserialise flat int32 array back into [(hash, offset), ...]
            flat = np.frombuffer(hashes_bytes, dtype=np.int32)
            if len(flat) < 2:
                continue
            pairs      = flat.reshape(-1, 2)
            query_map  = {int(h): int(off) for h, off in pairs}
            hash_list  = list(query_map.keys())

            queries_total += 1

            with get_conn() as conn:
                vault_size = _vault_size(conn)

                if vault_size == 0:
                    now = time.time()
                    if now - last_vault_warn >= 120.0:
                        log.warning(fmt_vault_empty_periodic(),
                                    extra=_extra(PROC))
                        last_vault_warn = now
                    continue

                t_query = time.perf_counter()
                db_rows = _query_vault(conn, hash_list)
                query_ms = (time.perf_counter() - t_query) * 1000

            if not db_rows:
                now = time.time()
                if now - last_no_match >= 10.0:
                    log.debug(fmt_no_candidates(vault_size),
                              extra=_extra(PROC))
                    last_no_match = now
                continue

            # Score all candidates
            scores = _time_coherence_score(query_map, db_rows)
            if not scores:
                continue

            # Pick the best candidate
            best_id, (best_score, best_name, best_dur) = max(
                scores.items(), key=lambda kv: kv[1][0]
            )

            now = time.time()

            # ── MATCH ─────────────────────────────────────────
            if best_score >= threshold:
                # Cooldown check
                last_match_time = cooldown_map.get(best_id, 0.0)
                if now - last_match_time < cooldown:
                    log.debug(
                        "Suppressed (cooldown) — \"%s\"  %.0fs remaining",
                        best_name, cooldown - (now - last_match_time),
                        extra=_extra(PROC)
                    )
                    continue

                cooldown_map[best_id] = now
                matches_total += 1

                log.info(
                    fmt_match(best_name, best_score, threshold, best_dur),
                    extra=_extra(PROC)
                )

                payload = json.dumps({
                    "ad_id":    best_id,
                    "ad_name":  best_name,
                    "duration": best_dur,
                    "score":    best_score,
                    "query_ms": round(query_ms, 1),
                    "ts":       now,
                }).encode()

                publisher.send_multipart([TOPIC_MATCH, payload])

            # ── NEAR MISS ─────────────────────────────────────
            elif best_score >= near_miss_min:
                log.info(
                    fmt_near_miss(best_name, best_score, threshold),
                    extra=_extra(PROC)
                )
                payload = json.dumps({
                    "ad_id":   best_id,
                    "ad_name": best_name,
                    "score":   best_score,
                    "ts":      now,
                }).encode()
                publisher.send_multipart([TOPIC_NEAR_MISS, payload])

            # ── STRIKE ────────────────────────────────────────
            elif best_score >= strike_min:
                log.info(
                    fmt_strike(best_name, best_score, threshold),
                    extra=_extra(PROC)
                )
                payload = json.dumps({
                    "ad_id":   best_id,
                    "ad_name": best_name,
                    "score":   best_score,
                    "ts":      now,
                }).encode()
                publisher.send_multipart([TOPIC_STRIKE, payload])

            # ── EXILE CLEANUP ─────────────────────────────────────────
            # Periodically purge cooldown_map entries for exiled ads.
            if len(cooldown_map) > 0 and queries_total % 50 == 0:
                try:
                    with get_conn() as conn:
                        active_ids = {
                            row[0] for row in
                            conn.execute(
                                "SELECT id FROM ads WHERE is_active = 1"
                            ).fetchall()
                        }
                    # Remove any cooldown entries for exiled ads
                    exiled = [k for k in cooldown_map if k not in active_ids]
                    for k in exiled:
                        del cooldown_map[k]
                        log.info(
                            "🗑 Cleared cooldown for exiled ad_id=%d", k,
                            extra=_extra(PROC)
                        )
                except Exception:
                    pass

            # Heartbeat
            if now - last_heartbeat >= 60.0:
                log.info(
                    "♥  queries=%d  matches=%d  vault=%d  last_query=%.1fms",
                    queries_total, matches_total, vault_size, query_ms,
                    extra=_extra(PROC)
                )
                last_heartbeat = now

        except zmq.ZMQError as exc:
            if running:
                log.error("ZMQ error: %s", exc, extra=_extra(PROC))
            break
        except sqlite3.Error as exc:
            log.error("DB error: %s", exc, extra=_extra(PROC))
            time.sleep(0.5)
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
