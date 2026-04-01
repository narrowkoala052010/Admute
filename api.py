# -----------------------------------------------------------------------------
# AdMute — Signal Intelligence for the Living Room
# Copyright (C) 2026 Carlos C
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# FULLY TRACKED PRIOR ART RECORD: This logic implements the "Analog Hole" 
# defense via acoustic fingerprinting. 
# -----------------------------------------------------------------------------

"""
AdMute v6 — Local API
aiohttp server on port 5001.
Implements the Absolute Timeline Math for flawless ingestion.
"""

import os
import sys
import json
import time
import hmac
import wave
import hashlib
import secrets
import logging
import sqlite3
import asyncio
import signal
from pathlib import Path

import aiohttp
from aiohttp import web
import aiofiles
import zmq
import numpy as np

BASE = Path(__file__).parent.resolve()
sys.path.insert(0, str(BASE))

from config  import load_config
from db      import get_conn, state_get, state_set, DB_PATH
from bus     import MUTE_CTRL, AUDIO_CTRL
from console import setup_logging, _extra
from fingerprint_worker import generate_hashes, _init_filter, _apply_filter, _normalise_peak
from matcher import query_vault, time_coherence_score

PROC = "API"


# ── SECRET MANAGEMENT ─────────────────────────────────────────────────────────

def _get_or_create_secret() -> str:
    secret = state_get("api_secret")
    if not secret:
        secret = secrets.token_hex(32)
        state_set("api_secret", secret)
        print()
        print("═" * 60)
        print("  AdMute API — First Run Setup")
        print("  Your API secret has been generated:")
        print()
        print(f"  {secret}")
        print()
        print("  Copy this into the web UI setup screen.")
        print("  It is stored in the database and won't change.")
        print("═" * 60)
        print()
    return secret


# ── HMAC AUTHENTICATION ───────────────────────────────────────────────────────

def _verify_hmac(request_method: str, request_path: str,
                 body: bytes, auth_header: str,
                 secret: str) -> bool:
    try:
        scheme, token     = auth_header.split(" ", 1)
        if scheme.upper() != "HMAC":
            return False
        timestamp_str, sig = token.split(".", 1)
        timestamp          = int(timestamp_str)

        if abs(time.time() - timestamp) > 120:
            return False

        message = (timestamp_str + request_method +
                   request_path + body.decode(errors="replace"))
        expected = hmac.new(
            secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(expected, sig)
    except Exception:
        return False

@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        return web.Response(
            status=200,
            headers={
                "Access-Control-Allow-Origin":  "*",
                "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Authorization, Content-Type",
                "Access-Control-Max-Age":       "86400",
            }
        )

    response = await handler(request)
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response

def auth_middleware(secret: str):
    @web.middleware
    async def _middleware(request: web.Request, handler):
        if request.path == "/api/health":
            return await handler(request)

        if "/audio" in request.path:
            return await handler(request)

        if not request.path.startswith("/api/"):
            return await handler(request)

        auth = request.headers.get("Authorization", "")
        body = await request.read()

        if not _verify_hmac(request.method, request.path_qs,
                             body, auth, secret):
            return web.json_response(
                {"error": "Unauthorized"}, status=401
            )

        request._stored_body = body
        return await handler(request)

    return _middleware


async def _body(request: web.Request) -> bytes:
    return getattr(request, "_stored_body", None) or await request.read()


async def _json(request: web.Request) -> dict:
    raw = await _body(request)
    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return {}


# ── ZMQ HELPERS ───────────────────────────────────────────────────────────────

def _zmq_send(address: str, command: dict,
              timeout_ms: int = 5000) -> dict:
    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(address)
    try:
        socket.send_string(json.dumps(command))
        return json.loads(socket.recv_string())
    except zmq.ZMQError as e:
        return {"ok": False, "error": f"ZMQ error: {e}"}
    finally:
        socket.close()
        ctx.destroy(linger=0)


async def _cmd_mute(command: dict) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, _zmq_send, MUTE_CTRL, command
    )


async def _cmd_audio(command: dict) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, _zmq_send, AUDIO_CTRL, command
    )


# ── FINGERPRINT INGESTION (ABSOLUTE TIMELINE FIX) ─────────────────────────────
def _ingest_recording(recording_id: int,
                      cfg, log: logging.Logger) -> dict:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM recordings WHERE id = ?",
            (recording_id,)
        ).fetchone()

    if not row:
        return {"ok": False, "error": "Recording not found"}
    if row["status"] == "ingested":
        return {"ok": False, "error": "Already ingested"}

    filepath = Path(row["file_path"])
    if not filepath.exists():
        return {"ok": False, "error": f"WAV file not found: {filepath}"}

    try:
        with wave.open(str(filepath), 'rb') as wf:
            frames    = wf.readframes(wf.getnframes())
            framerate = wf.getframerate()

        audio_int = np.frombuffer(frames, dtype=np.int32)
        audio     = audio_int.astype(np.float32) / 2_147_483_647.0
        duration  = len(audio) / framerate
    except Exception as e:
        return {"ok": False, "error": f"Failed to read WAV: {e}"}

    _init_filter(cfg.audio.sample_rate)
    
    # Replicate the EXACT windowing math used by the live audio_capture.py
    chunks_per_window = int(cfg.audio.sample_rate / cfg.audio.chunk_size * cfg.audio.buffer_seconds)
    half_window = max(1, chunks_per_window // 2)
    
    window_samples = chunks_per_window * cfg.audio.chunk_size
    hop_samples = half_window * cfg.audio.chunk_size
    
    # Calculate the STFT frame jump
    stft_hop = int(cfg.fingerprint.nperseg * (1.0 - cfg.fingerprint.noverlap_ratio))

    all_hashes: list[tuple[int, int]] = []
    start = 0
    
    while start + window_samples <= len(audio):
        window = audio[start:start + window_samples].copy()
        filtered   = _apply_filter(window)
        normalised = _normalise_peak(filtered, cfg.audio.silence_threshold)
        
        if normalised is not None:
            window_hashes = generate_hashes(normalised, cfg.fingerprint, cfg.audio.sample_rate)
            
            # THE TIME MACHINE FIX: Calculate where we actually are in the ad
            absolute_frame_offset = start // stft_hop
            
            # Shift every local hash frame to its true absolute timeline position
            for h_val, local_frame in window_hashes:
                all_hashes.append((h_val, local_frame + absolute_frame_offset))
                
        start += hop_samples

    hashes = all_hashes

    if len(hashes) < 50:
        return {"ok": False, "error": f"Only {len(hashes)} hashes generated — too few to match reliably."}

    query_map = {int(h): int(off) for h, off in hashes[:250]}
    hash_list = list(query_map.keys())

    best_parent_id = None
    best_parent_name = None

    with get_conn() as conn:
        db_rows = query_vault(conn, hash_list)
        scores = time_coherence_score(query_map, db_rows)

        if scores:
            best_id, (best_score, best_name, best_dur, best_delta) = max(
                scores.items(), key=lambda kv: kv[1][0]
            )
            required_score = max(cfg.match.confidence_threshold, int(len(query_map) * 0.40))
            
            if best_score >= required_score:
                best_parent_id = best_id
                best_parent_name = best_name
                log.info("🔗 Pre-check matched \"%s\" (Score: %d/%d). Flagging for review.", 
                         best_name, best_score, required_score, extra=_extra(PROC))

    try:
        with get_conn() as conn:
            name = (row["streaming_service"] or "Unknown") + " Ad " + str(int(time.time()))[-4:]
            
            is_active = 0 if best_parent_id else 1

            conn.execute(
                """INSERT INTO ads
                   (name, duration_seconds, streaming_service, category, source, hash_count, is_active, parent_ad_id)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (name, round(duration, 2), row["streaming_service"] or "Unknown",
                 row["category"] or "Uncategorized", "mic", len(hashes),
                 is_active, best_parent_id)
            )
            ad_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            conn.executemany(
                "INSERT INTO hashes (hash_value, time_offset, ad_id) VALUES (?,?,?)",
                [(h, off, ad_id) for h, off in hashes]
            )

            rec_status = "pending_review" if best_parent_id else "ingested"
            conn.execute(
                "UPDATE recordings SET status=?, ad_id=?, pending_link_ad_id=? WHERE id=?",
                (rec_status, ad_id, best_parent_id, recording_id)
            )
            conn.commit()

        return {
            "ok":         True,
            "ad_id":      ad_id,
            "hash_count": len(hashes),
            "duration":   round(duration, 2),
            "name":       name,
            "status":     rec_status,
            "parent":     best_parent_name
        }
    except sqlite3.Error as e:
        return {"ok": False, "error": f"DB error: {e}"}

# ── ROUTE HANDLERS ────────────────────────────────────────────────────────────

async def health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "uptime": round(time.time() - request.app["start_time"], 1),
    })

async def get_status(request: web.Request) -> web.Response:
    mute_status = await _cmd_mute({"cmd": "status"})
    mic_active  = state_get("mic_active", "1") == "1"
    last_snr    = state_get("last_snr_db")
    last_class  = state_get("last_snr_class")
    backend     = state_get("mute_backend", "ir")

    with get_conn() as conn:
        vault_size = conn.execute(
            "SELECT COUNT(*) FROM ads WHERE is_active=1"
        ).fetchone()[0]

    return web.json_response({
        "is_muted":       mute_status.get("is_muted", False),
        "current_ad_name":mute_status.get("current_ad_name"),
        "current_ad_id":  mute_status.get("current_ad_id"),
        "mic_active":     mic_active,
        "mute_backend":   backend,
        "vault_size":     vault_size,
        "snr_db":         float(last_snr)  if last_snr  else None,
        "snr_class":      last_class,
        "uptime":         round(time.time() - request.app["start_time"], 1),
    })

async def get_config(request: web.Request) -> web.Response:
    cfg = load_config()
    return web.json_response({
        "audio": {
            "sample_rate":    cfg.audio.sample_rate,
            "chunk_size":     cfg.audio.chunk_size,
            "buffer_seconds": cfg.audio.buffer_seconds,
        },
        "match": {
            "confidence_threshold": cfg.match.confidence_threshold,
            "cooldown_seconds":     cfg.match.cooldown_seconds,
            "near_miss_ratio":      cfg.match.near_miss_ratio,
        },
        "mute": {
            "backend":               cfg.mute.backend,
            "safety_margin_seconds": cfg.mute.safety_margin_seconds,
            "ir_remote_name":        cfg.mute.ir_remote_name,
            "ir_key_mute":           cfg.mute.ir_key_mute,
        },
        "api": {"port": cfg.api.port},
    })

async def mute_toggle(request: web.Request) -> web.Response:
    result = await _cmd_mute({"cmd": "mute_toggle"})
    return web.json_response(result)

async def false_positive(request: web.Request) -> web.Response:
    result = await _cmd_mute({"cmd": "false_positive"})
    return web.json_response(result)

async def mic_toggle(request: web.Request) -> web.Response:
    result = await _cmd_audio({"cmd": "mic_toggle"})
    if result.get("ok"):
        state_set("mic_active", "1" if result["mic_active"] else "0")
    return web.json_response(result)

async def snr_test(request: web.Request) -> web.Response:
    result = await _cmd_audio({"cmd": "snr_test"})
    log = request.app["log"]
    if not result.get("ok"):
        log.error("SNR test ZMQ call failed: %s", result, extra=_extra(PROC))
    else:
        log.info("SNR test: %.1fdB [%s]", result.get("snr_db", 0), result.get("classification", "?"), extra=_extra(PROC))
    
    if result.get("ok"):
        state_set("last_snr_db",    str(result["snr_db"]))
        state_set("last_snr_class", result["classification"])
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO snr_log (snr_db, classification, placement_note) VALUES (?,?,?)",
                (result["snr_db"], result["classification"], result["note"])
            )
            conn.commit()
    return web.json_response(result)

async def record_start(request: web.Request) -> web.Response:
    result = await _cmd_audio({"cmd": "start_record"})
    state_set("is_recording", "1")
    return web.json_response(result)

async def record_stop(request: web.Request) -> web.Response:
    state_set("is_recording", "0")
    result = await _cmd_audio({"cmd": "stop_record"})
    if not result.get("ok"):
        return web.json_response(result, status=500)

    filepath = result["filepath"]
    duration = result["duration"]

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO recordings (file_path, duration_seconds, status) VALUES (?,?,?)",
            (filepath, duration, "pending_review")
        )
        rec_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()

    return web.json_response({
        "ok":           True,
        "recording_id": rec_id,
        "duration":     duration,
        "filepath":     filepath,
    })

async def list_recordings(request: web.Request) -> web.Response:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT r.id, r.ad_id, r.file_path, r.duration_seconds,
                      r.status, r.streaming_service, r.category,
                      r.notes, r.recorded_at, r.pending_link_ad_id,
                      a.name as parent_name
               FROM recordings r
               LEFT JOIN ads a ON r.pending_link_ad_id = a.id
               ORDER BY r.recorded_at DESC"""
        ).fetchall()

    return web.json_response([{
        "id":                 r["id"],
        "ad_id":              r["ad_id"],
        "filename":           Path(r["file_path"]).name,
        "duration_seconds":   r["duration_seconds"],
        "status":             r["status"],
        "streaming_service":  r["streaming_service"],
        "category":           r["category"],
        "notes":              r["notes"],
        "recorded_at":        r["recorded_at"],
        "pending_link_ad_id": r["pending_link_ad_id"],
        "parent_name":        r["parent_name"],
    } for r in rows])

async def review_accept(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        conn.execute("UPDATE recordings SET status='ingested' WHERE id=?", (rec_id,))
        conn.execute("UPDATE ads SET is_active=1 WHERE id = (SELECT ad_id FROM recordings WHERE id=?)", (rec_id,))
        conn.commit()
    return web.json_response({"ok": True})

async def review_reject(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        conn.execute("UPDATE recordings SET status='ingested', pending_link_ad_id=NULL WHERE id=?", (rec_id,))
        conn.execute("UPDATE ads SET is_active=1, parent_ad_id=NULL WHERE id = (SELECT ad_id FROM recordings WHERE id=?)", (rec_id,))
        conn.commit()
    return web.json_response({"ok": True})

async def update_recording(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    data   = await _json(request)
    allowed = {"streaming_service", "category", "notes"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return web.json_response({"error": "No valid fields"}, status=400)
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values     = list(updates.values()) + [rec_id]
    with get_conn() as conn:
        conn.execute(f"UPDATE recordings SET {set_clause} WHERE id=?", values)
        conn.commit()
    return web.json_response({"ok": True})

async def ingest_recording(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    cfg    = request.app["cfg"]
    log    = request.app["log"]

    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _ingest_recording, rec_id, cfg, log)

    status = 200 if result["ok"] else 422
    return web.json_response(result, status=status)

async def get_recording_audio(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute("SELECT file_path FROM recordings WHERE id=?", (rec_id,)).fetchone()
    if not row:
        return web.json_response({"error": "Not found"}, status=404)
    filepath = Path(row["file_path"])
    if not filepath.exists():
        return web.json_response({"error": "File missing"}, status=404)
    return web.FileResponse(filepath, headers={"Content-Type": "audio/wav", "Content-Disposition": f"inline; filename={filepath.name}"})

async def delete_recording(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM recordings WHERE id=?", (rec_id,)).fetchone()
    if not row:
        return web.json_response({"error": "Not found"}, status=404)
    fp = Path(row["file_path"])
    if fp.exists():
        fp.unlink()
    with get_conn() as conn:
        if row["ad_id"]:
            conn.execute("DELETE FROM hashes WHERE ad_id=?", (row["ad_id"],))
            conn.execute("DELETE FROM ads WHERE id=?", (row["ad_id"],))
        conn.execute("DELETE FROM recordings WHERE id=?", (rec_id,))
        conn.commit()
    return web.json_response({"ok": True})

async def list_ads(request: web.Request) -> web.Response:
    with get_conn() as conn:
        rows = conn.execute("""SELECT id, name, duration_seconds, streaming_service, category, is_active, hash_count, source, created_at FROM ads ORDER BY created_at DESC""").fetchall()
    return web.json_response([{"id": r["id"], "name": r["name"], "duration_seconds": r["duration_seconds"], "streaming_service": r["streaming_service"], "category": r["category"], "is_active": bool(r["is_active"]), "hash_count": r["hash_count"], "source": r["source"], "created_at": r["created_at"]} for r in rows])

async def update_ad(request: web.Request) -> web.Response:
    ad_id  = int(request.match_info["id"])
    data   = await _json(request)
    allowed = {"name", "streaming_service", "category"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return web.json_response({"error": "No valid fields"}, status=400)
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values     = list(updates.values()) + [ad_id]
    with get_conn() as conn:
        conn.execute(f"UPDATE ads SET {set_clause} WHERE id=?", values)
        conn.commit()
    return web.json_response({"ok": True})

async def deactivate_ad(request: web.Request) -> web.Response:
    ad_id = int(request.match_info["id"])
    with get_conn() as conn:
        conn.execute("UPDATE ads SET is_active=0 WHERE id=?", (ad_id,))
        conn.commit()
    return web.json_response({"ok": True})

async def purge_ad(request: web.Request) -> web.Response:
    ad_id = int(request.match_info["id"])
    data  = await _json(request)
    if not data.get("confirm"):
        with get_conn() as conn:
            ad = conn.execute("SELECT * FROM ads WHERE id=?", (ad_id,)).fetchone()
            if not ad:
                return web.json_response({"error": "Ad not found"}, status=404)
            hash_count  = conn.execute("SELECT COUNT(*) FROM hashes WHERE ad_id=?", (ad_id,)).fetchone()[0]
            mute_count  = conn.execute("SELECT COUNT(*) FROM mute_log WHERE ad_id=?", (ad_id,)).fetchone()[0]
        return web.json_response({"ok": False, "requires_confirm": True, "ad_name": ad["name"], "impact": {"hashes_to_delete": hash_count, "mute_history_entries": mute_count, "warning": f"This will permanently delete \"{ad['name']}\" and its {hash_count} fingerprint hashes. It will no longer be detected. This cannot be undone."}})
    with get_conn() as conn:
        conn.execute("DELETE FROM ads WHERE id=?", (ad_id,))
        conn.commit()
    request.app["log"].info("Purged ad_id=%d and all its hashes", ad_id, extra=_extra(PROC))
    return web.json_response({"ok": True, "purged": True})

async def get_ad_audio(request: web.Request) -> web.Response:
    ad_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute("SELECT r.file_path FROM recordings r WHERE r.ad_id=? AND r.status='ingested' LIMIT 1", (ad_id,)).fetchone()
    if not row:
        return web.json_response({"error": "No audio file for this ad"}, status=404)
    fp = Path(row["file_path"])
    if not fp.exists():
        return web.json_response({"error": "File missing"}, status=404)
    return web.FileResponse(fp, headers={"Content-Type": "audio/wav"})

async def stats_summary(request: web.Request) -> web.Response:
    with get_conn() as conn:
        secs = conn.execute("SELECT COALESCE(SUM(duration_actual), 0) FROM mute_log WHERE muted_at >= date('now','start of month') AND was_false_positive = 0").fetchone()[0]
        mute_count = conn.execute("SELECT COUNT(*) FROM mute_log WHERE muted_at >= date('now','start of month') AND was_false_positive = 0").fetchone()[0]
        fp_count = conn.execute("SELECT COUNT(*) FROM mute_log WHERE muted_at >= date('now','start of month') AND was_false_positive = 1").fetchone()[0]
        vault_size = conn.execute("SELECT COUNT(*) FROM ads WHERE is_active=1").fetchone()[0]
        top_ads = conn.execute("""SELECT m.ad_id, m.ad_name_snapshot, COUNT(*) as cnt, a.category, a.streaming_service, (SELECT notes FROM recordings WHERE ad_id = a.id LIMIT 1) as notes FROM mute_log m LEFT JOIN ads a ON m.ad_id = a.id WHERE m.muted_at >= date('now','start of month') AND m.was_false_positive = 0 AND m.ad_name_snapshot IS NOT NULL GROUP BY m.ad_name_snapshot, m.ad_id, a.category, a.streaming_service ORDER BY cnt DESC LIMIT 5""").fetchall()
    return web.json_response({"minutes_saved_this_month": round(secs / 60.0, 1), "mute_events_this_month": mute_count, "false_positives_this_month": fp_count, "vault_size": vault_size, "top_muted_ads": [{"id": r["ad_id"], "name": r["ad_name_snapshot"], "category": r["category"], "streaming_service": r["streaming_service"], "notes": r["notes"], "count": r["cnt"]} for r in top_ads]})

async def stats_mute_log(request: web.Request) -> web.Response:
    page  = int(request.rel_url.query.get("page",  1))
    limit = int(request.rel_url.query.get("limit", 20))
    fp_only = request.rel_url.query.get("fp_only", "false") == "true"
    offset  = (page - 1) * limit
    where = "WHERE was_false_positive=1" if fp_only else ""
    with get_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM mute_log {where}").fetchone()[0]
        rows = conn.execute(f"""SELECT m.id, m.ad_id, m.ad_name_snapshot, m.muted_at, m.unmuted_at, m.duration_actual, m.mute_method, m.confidence_score, m.was_false_positive, a.category, a.streaming_service, (SELECT notes FROM recordings WHERE ad_id = a.id LIMIT 1) as notes FROM mute_log m LEFT JOIN ads a ON m.ad_id = a.id {where} ORDER BY m.muted_at DESC LIMIT ? OFFSET ?""", (limit, offset)).fetchall()
    return web.json_response({"total": total, "page": page, "limit": limit, "items": [{"id": r["id"], "ad_id": r["ad_id"], "ad_name": r["ad_name_snapshot"], "category": r["category"], "streaming_service": r["streaming_service"], "notes": r["notes"], "muted_at": r["muted_at"], "unmuted_at": r["unmuted_at"], "duration_actual": r["duration_actual"], "mute_method": r["mute_method"], "confidence_score": r["confidence_score"], "was_false_positive": bool(r["was_false_positive"])} for r in rows]})

async def stats_snr_history(request: web.Request) -> web.Response:
    with get_conn() as conn:
        rows = conn.execute("SELECT snr_db, classification, placement_note, measured_at FROM snr_log ORDER BY measured_at DESC LIMIT 30").fetchall()
    return web.json_response([{"snr_db": r["snr_db"], "classification": r["classification"], "note": r["placement_note"], "measured_at": r["measured_at"]} for r in rows])

async def serve_index(request: web.Request) -> web.Response:
    index = BASE / "static" / "index.html"
    return web.FileResponse(index)

def create_app(cfg, log: logging.Logger, secret: str) -> web.Application:
    app = web.Application(middlewares=[cors_middleware, auth_middleware(secret)])
    app["cfg"]        = cfg
    app["log"]        = log
    app["start_time"] = time.time()
    app["secret"]     = secret
    static_dir = BASE / "static"
    static_dir.mkdir(exist_ok=True)
    app.router.add_get ("/", serve_index)
    app.router.add_get ("/api/health", health)
    app.router.add_get ("/api/status", get_status)
    app.router.add_get ("/api/config", get_config)
    app.router.add_post("/api/mute/toggle", mute_toggle)
    app.router.add_post("/api/mute/false-positive", false_positive)
    app.router.add_post("/api/mic/toggle", mic_toggle)
    app.router.add_post("/api/snr-test", snr_test)
    app.router.add_post("/api/record/start", record_start)
    app.router.add_post("/api/record/stop", record_stop)
    app.router.add_get ("/api/recordings", list_recordings)
    app.router.add_get ("/api/recordings/{id}/audio", get_recording_audio)
    app.router.add_route("PATCH",  "/api/recordings/{id}", update_recording)
    app.router.add_post("/api/recordings/{id}/ingest", ingest_recording)
    app.router.add_post("/api/recordings/{id}/link/accept", review_accept)
    app.router.add_post("/api/recordings/{id}/link/reject", review_reject)
    app.router.add_route("DELETE", "/api/recordings/{id}", delete_recording)
    app.router.add_get ("/api/ads", list_ads)
    app.router.add_route("PATCH",  "/api/ads/{id}", update_ad)
    app.router.add_post("/api/ads/{id}/deactivate", deactivate_ad)
    app.router.add_post("/api/ads/{id}/purge", purge_ad)
    app.router.add_get ("/api/ads/{id}/audio", get_ad_audio)
    app.router.add_get ("/api/stats/summary", stats_summary)
    app.router.add_get ("/api/stats/mute-log", stats_mute_log)
    app.router.add_get ("/api/stats/snr-history", stats_snr_history)
    for path in ["/api/health", "/api/status", "/api/config", "/api/mute/toggle", "/api/mute/false-positive", "/api/mic/toggle", "/api/snr-test", "/api/record/start", "/api/record/stop", "/api/recordings", "/api/recordings/{id}", "/api/recordings/{id}/audio", "/api/recordings/{id}/ingest", "/api/ads", "/api/ads/{id}", "/api/ads/{id}/deactivate", "/api/ads/{id}/purge", "/api/ads/{id}/audio", "/api/stats/summary", "/api/stats/mute-log", "/api/stats/snr-history","/api/recordings/{id}/link/accept", "/api/recordings/{id}/link/reject"]:
        app.router.add_route("OPTIONS", path, lambda r: web.Response(status=200))
    app.router.add_static("/static", static_dir, name="static", show_index=False)
    return app

def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    cfg    = load_config()
    log    = setup_logging(PROC, log_level, log_dir)
    secret = _get_or_create_secret()
    app    = create_app(cfg, log, secret)
    log.info("Starting on port %d", cfg.api.port, extra=_extra(PROC))
    web.run_app(app, host="0.0.0.0", port=cfg.api.port, print=None, access_log=None)

if __name__ == "__main__":
    run()