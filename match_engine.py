"""
AdMute v6 — Process 3: MatchEngine
Predictive matching using a Markov Transition Matrix and RAM cache.
Tracks telemetry trace_ids for distributed debugging.
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
from matcher import query_vault, time_coherence_score

PROC = "MATCH"


def _vault_size(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM ads WHERE is_active = 1").fetchone()
    return row[0] if row else 0

def _update_markov_matrix(conn: sqlite3.Connection, source_id: int, target_id: int, log, trace_id: str):
    """Updates the transition probability between two ads."""
    if source_id == target_id:
        return # Don't predict self-loops
    try:
        conn.execute(
            """INSERT INTO markov_transitions (source_ad_id, target_ad_id, transition_count)
               VALUES (?, ?, 1)
               ON CONFLICT(source_ad_id, target_ad_id) DO UPDATE 
               SET transition_count = transition_count + 1,
                   last_seen_at = CURRENT_TIMESTAMP""",
            (source_id, target_id)
        )
        conn.commit()
        log.debug("Updated Markov matrix: %d -> %d", source_id, target_id, extra=_extra(PROC, trace_id))
    except sqlite3.Error as e:
        log.error("Markov DB update failed: %s", e, extra=_extra(PROC, trace_id))

def _prefetch_ram_cache(conn: sqlite3.Connection, source_id: int, log, trace_id: str) -> dict:
    """Pre-loads the top 3 predicted ads into a blazing-fast RAM dictionary."""
    try:
        # Find top 3 predicted targets
        targets = conn.execute(
            """SELECT target_ad_id FROM markov_transitions 
               WHERE source_ad_id = ? 
               ORDER BY transition_count DESC LIMIT 3""",
            (source_id,)
        ).fetchall()
        
        target_ids = [t[0] for t in targets]
        if not target_ids:
            return {}

        # Load their hashes into memory
        placeholders = ",".join("?" * len(target_ids))
        rows = conn.execute(
            f"""SELECT h.hash_value, h.time_offset, h.ad_id, a.name, a.duration_seconds 
                FROM hashes h JOIN ads a ON h.ad_id = a.id 
                WHERE a.id IN ({placeholders}) AND a.is_active = 1""",
            target_ids
        ).fetchall()

        cache = defaultdict(list)
        for r in rows:
            cache[r["hash_value"]].append(dict(r))
            
        log.info("Prefetched %d hashes for %d predicted ads into RAM cache", 
                 len(rows), len(target_ids), extra=_extra(PROC, trace_id))
        return cache
    except sqlite3.Error as e:
        log.error("RAM Cache prefetch failed: %s", e, extra=_extra(PROC, trace_id))
        return {}


def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    cfg = load_config()
    log = setup_logging(PROC, log_level, log_dir)

    threshold     = cfg.match.confidence_threshold
    near_miss_min = int(threshold * cfg.match.near_miss_ratio)
    strike_min    = cfg.match.strike_min
    cooldown      = cfg.match.cooldown_seconds
    ttl_seconds   = cfg.match.markov_ttl_seconds

    # ── ZMQ ──────────────────────────────────────────────────
    ctx      = zmq.Context()
    receiver = ctx.socket(zmq.PULL)
    receiver.set_hwm(16)
    receiver.bind(FINGER_PUSH)

    publisher = ctx.socket(zmq.PUB)
    publisher.bind(MATCH_PUB)

    time.sleep(0.2)   # let subscribers connect

    # ── State ─────────────────────────────────────────────────
    cooldown_map: dict[int, float] = {}   
    queries_total  = 0
    matches_total  = 0
    last_heartbeat = time.time()
    last_vault_warn= 0.0
    last_no_match  = 0.0 

    # Markov State
    last_matched_ad_id = None
    last_match_ts      = 0.0
    ram_cache          = {}  # Hash Value -> List of DbRows

    # ── Graceful shutdown ─────────────────────────────────────
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    log.info(
        "Ready — threshold=%d  near_miss≥%d  strike≥%d  ttl=%.0fs",
        threshold, near_miss_min, strike_min, ttl_seconds,
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
            trace_id = meta.get("trace_id")

            # ── MARKOV TTL CHECK ──
            now = time.time()
            if ram_cache and (now - last_match_ts > ttl_seconds):
                log.info("Markov TTL expired (%.0fs silence). Flushing RAM cache.", 
                         now - last_match_ts, extra=_extra(PROC, trace_id))
                ram_cache = {}
                last_matched_ad_id = None

            flat = np.frombuffer(hashes_bytes, dtype=np.int32)
            if len(flat) < 2:
                continue
            pairs      = flat.reshape(-1, 2)
            query_map  = {int(h): int(off) for h, off in pairs}
            hash_list  = list(query_map.keys())

            queries_total += 1
            t_query = time.perf_counter()
            best_id = None
            best_score = 0
            best_name = ""
            best_dur = 0.0
            best_delta = 0
            
            # 1. FAST PATH: Check RAM Cache First
            if ram_cache:
                ram_rows = []
                for h in hash_list:
                    if h in ram_cache:
                        ram_rows.extend(ram_cache[h])
                
                if ram_rows:
                    ram_scores = time_coherence_score(query_map, ram_rows)
                    if ram_scores:
                        r_id, (r_score, r_name, r_dur, r_delta) = max(ram_scores.items(), key=lambda kv: kv[1][0])
                        # We allow slightly lower threshold for predicted ads!
                        if r_score >= int(threshold * 0.85): 
                            best_id, best_score, best_name, best_dur, best_delta = r_id, r_score, r_name, r_dur, r_delta
                            log.debug("⚡ RAM Cache Match: %s (Score: %d)", best_name, best_score, extra=_extra(PROC, trace_id))

            # 2. SLOW PATH: Fallback to SQLite Disk Scan
            if not best_id:
                with get_conn() as conn:
                    vault_size = _vault_size(conn)
                    if vault_size == 0:
                        if now - last_vault_warn >= 120.0:
                            log.warning(fmt_vault_empty_periodic(), extra=_extra(PROC, trace_id))
                            last_vault_warn = now
                        continue

                    db_rows = query_vault(conn, hash_list)
                    
                    if not db_rows:
                        if now - last_no_match >= 10.0:
                            log.debug(fmt_no_candidates(vault_size), extra=_extra(PROC, trace_id))
                            last_no_match = now
                        continue

                    scores = time_coherence_score(query_map, db_rows)
                    if not scores:
                        continue

                    best_id, (best_score, best_name, best_dur, best_delta) = max(
                        scores.items(), key=lambda kv: kv[1][0]
                    )

            query_ms = (time.perf_counter() - t_query) * 1000

            # ── MATCH EVALUATION ─────────────────────────────────────────
            if best_score >= threshold:
                
                # Cooldown check
                last_matched = cooldown_map.get(best_id, 0.0)
                if now - last_matched < cooldown:
                    continue

                cooldown_map[best_id] = now
                matches_total += 1

                log.info(
                    fmt_match(best_name, best_score, threshold, best_dur),
                    extra=_extra(PROC, trace_id)
                )

                # Send Mute Command via ZMQ
                payload = json.dumps({
                    "trace_id": trace_id,
                    "ad_id":    best_id,
                    "ad_name":  best_name,
                    "duration": best_dur,
                    "delta":    best_delta,
                    "score":    best_score,
                    "query_ms": round(query_ms, 1),
                    "ts":       now,
                }).encode()
                publisher.send_multipart([TOPIC_MATCH, payload])

                # ── MARKOV PREDICTION & PREFETCH ──
                with get_conn() as conn:
                    if last_matched_ad_id:
                        _update_markov_matrix(conn, last_matched_ad_id, best_id, log, trace_id)
                    ram_cache = _prefetch_ram_cache(conn, best_id, log, trace_id)
                
                last_matched_ad_id = best_id
                last_match_ts      = now

            # ── NEAR MISS ─────────────────────────────────────
            elif best_score >= near_miss_min:
                log.info(fmt_near_miss(best_name, best_score, threshold), extra=_extra(PROC, trace_id))

            # ── STRIKE ────────────────────────────────────────
            elif best_score >= strike_min:
                log.info(fmt_strike(best_name, best_score, threshold), extra=_extra(PROC, trace_id))

            # Heartbeat
            if now - last_heartbeat >= 60.0:
                log.info(
                    "♥  queries=%d  matches=%d  last_query=%.1fms",
                    queries_total, matches_total, query_ms,
                    extra=_extra(PROC)
                )
                last_heartbeat = now

        except zmq.ZMQError as exc:
            if running:
                log.error("ZMQ error: %s", exc, extra=_extra(PROC))
            break
        except sqlite3.Error as exc:
            log.error("DB error: %s", exc, extra=_extra(PROC, locals().get("trace_id")))
            time.sleep(0.5)
        except Exception as exc:
            log.error("Unexpected error: %s", exc, extra=_extra(PROC, locals().get("trace_id")), exc_info=True)
            time.sleep(0.1)

    receiver.close()
    publisher.close()
    ctx.term()
    log.info("Stopped cleanly — %d total matches", matches_total, extra=_extra(PROC))


if __name__ == "__main__":
    run()