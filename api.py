"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Process 5 — Local API

aiohttp server on port 5001.  Serves the web UI static files and
exposes a REST API secured by HMAC-SHA256 signed requests.  Handles
recording, vault management, ingestion, stats, and mute control.
"""

"""
AdMute Light — Local API
aiohttp server on port 5001.

Routes
──────
GET    /                                  → serve frontend SPA
GET    /api/health                        → liveness probe (no auth)
GET    /api/status                        → daemon state summary
GET    /api/config                        → current config values

POST   /api/mute/toggle                   → manual mute/unmute
POST   /api/mute/false-positive           → flag current mute as FP

POST   /api/mic/toggle                    → enable / disable microphone
POST   /api/snr-test                      → run SNR placement test

POST   /api/record/start                  → begin ad recording
POST   /api/record/stop                   → stop and save recording

GET    /api/recordings                    → list all recordings
GET    /api/recordings/summon?ids=1,2,3   → fetch specific recordings (Workbench)
GET    /api/recordings/{id}/audio         → stream WAV file
PATCH  /api/recordings/{id}               → update metadata
POST   /api/recordings/{id}/ingest        → fingerprint and commit to vault
POST   /api/recordings/{id}/link/accept   → accept suggested variant link
POST   /api/recordings/{id}/link/reject   → reject suggested variant link
DELETE /api/recordings/{id}               → delete recording and its ad entry

GET    /api/ads                           → list vault entries
PATCH  /api/ads/{id}                      → update ad metadata
POST   /api/ads/{id}/reactivate           → restore deactivated ad
POST   /api/ads/{id}/deactivate           → deactivate ad (keep hashes)
POST   /api/ads/{id}/purge                → permanently delete ad + hashes
GET    /api/ads/{id}/audio                → stream source WAV for vault entry
POST   /api/ads/link                      → manually set parent-child relationship

GET    /api/stats/summary                 → monthly stats + top-muted ads
GET    /api/stats/mute-log                → paginated mute history
GET    /api/stats/snr-history             → last 30 SNR readings
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
import signal_utils
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
from fingerprint_worker import generate_hashes, _normalise_peak
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
        scheme, token      = auth_header.split(" ", 1)
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
            return web.json_response({"error": "Unauthorized"}, status=401)

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
    socket.setsockopt(zmq.LINGER,   0)
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
    return await loop.run_in_executor(None, _zmq_send, MUTE_CTRL, command)


async def _cmd_audio(command: dict) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _zmq_send, AUDIO_CTRL, command)


# ── FINGERPRINT INGESTION ─────────────────────────────────────────────────────

def _ingest_recording(recording_id: int, cfg, log: logging.Logger) -> dict:
    """
    Read a pending WAV recording, fingerprint it, and commit it to the vault.

    Pipeline:
      1. Load WAV → float32 numpy array.
      2. Slice into overlapping windows using the same hop size as the
         live sentinel so hashes are timeline-compatible.
      3. Apply the shared 300 Hz–16 kHz bandpass (signal_utils) — identical
         to what audio_capture.py and fingerprint_worker.py apply — then
         normalise and generate hashes.
      4. Anchor each hash's time_offset to the absolute timeline so hashes
         from different windows align correctly during matching.
      5. Run a quick variant pre-check against the vault.  If a confident
         match is found the new ad is linked to the existing one as a child
         (pending_link_ad_id).  The Workbench surfaces this as an
         accept / reject prompt.
      6. Commit the new ads row, all hashes, and update the recording status.

    Returns {"ok": True, "ad_id": int, "suggested_parent": int|None}
         or {"ok": False, "error": str}.
    """
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM recordings WHERE id = ?", (recording_id,)
        ).fetchone()

    if not row or row["status"] == "ingested":
        return {"ok": False, "error": "Recording not found or already ingested"}

    filepath = Path(row["file_path"])
    try:
        with wave.open(str(filepath), "rb") as wf:
            frames    = wf.readframes(wf.getnframes())
            framerate = wf.getframerate()

        # The WAV was saved by audio_capture.py as int32 with a 25× gain
        # already applied.  Divide by int32 max to recover [-1.0, 1.0] float.
        audio    = np.frombuffer(frames, dtype=np.int32).astype(np.float32) / 2_147_483_647.0
        duration = len(audio) / framerate
    except Exception as e:
        return {"ok": False, "error": f"WAV read error: {e}"}

    # Build the shared bandpass — same coefficients used by audio_capture.py
    # and fingerprint_worker.py so the ingest path is fully symmetric.
    b, a = signal_utils.build_bandpass(300.0, 16000.0, cfg.audio.sample_rate)

    stft_hop       = int(cfg.fingerprint.nperseg * (1.0 - cfg.fingerprint.noverlap_ratio))
    window_samples = (
        int(cfg.audio.sample_rate / cfg.audio.chunk_size * cfg.audio.buffer_seconds)
        * cfg.audio.chunk_size
    )
    hop_samples = window_samples // 2

    all_hashes = []
    start      = 0

    while start + window_samples <= len(audio):
        window = audio[start : start + window_samples].copy()

        # Apply the shared bandpass once — no double-filtering.
        clean_window = signal_utils.apply_filter(window, b, a)
        normalised   = _normalise_peak(clean_window, cfg.audio.silence_threshold)

        if normalised is not None:
            # Anchor local frame offsets to the absolute timeline so hashes
            # from every window are comparable during match scoring.
            absolute_offset = start // stft_hop
            for h_val, local_frame in generate_hashes(
                normalised, cfg.fingerprint, cfg.audio.sample_rate
            ):
                all_hashes.append((h_val, local_frame + absolute_offset))

        start += hop_samples

    if len(all_hashes) < 50:
        return {"ok": False, "error": "Signal too weak to generate fingerprint."}

    # ── Variant pre-check (Workbench suggestion) ──────────────────────────────
    # Score the first 250 hashes against the vault.  A confident hit means
    # this recording is likely a variant of an existing ad; the Workbench
    # will prompt the user to accept or reject the link.
    query_map      = {int(h): int(off) for h, off in all_hashes[:250]}
    best_parent_id = None

    with get_conn() as conn:
        db_rows = query_vault(conn, list(query_map.keys()))
        scores  = time_coherence_score(query_map, db_rows)
        if scores:
            best_id, (best_score, *_) = max(
                scores.items(), key=lambda kv: kv[1][0]
            )
            if best_score >= cfg.match.confidence_threshold:
                best_parent_id = best_id

    # ── Commit to vault ───────────────────────────────────────────────────────
    try:
        with get_conn() as conn:
            svc  = row["streaming_service"] or "Unknown"
            name = f"{svc} Ad {str(int(time.time()))[-4:]}"

            conn.execute(
                """INSERT INTO ads
                       (name, duration_seconds, streaming_service, category,
                        source, hash_count, is_active, parent_ad_id)
                   VALUES (?,?,?,?,?,?,1,?)""",
                (name, round(duration, 2), svc, row["category"],
                 "mic", len(all_hashes), best_parent_id)
            )
            ad_id = conn.execute(
                "SELECT last_insert_rowid()"
            ).fetchone()[0]

            conn.executemany(
                "INSERT INTO hashes (hash_value, time_offset, ad_id) VALUES (?,?,?)",
                [(h, off, ad_id) for h, off in all_hashes]
            )
            conn.execute(
                "UPDATE recordings SET status='ingested', ad_id=?, "
                "pending_link_ad_id=? WHERE id=?",
                (ad_id, best_parent_id, recording_id)
            )
            conn.commit()

        log.info(
            "Ingested recording %d → ad_id=%d  hashes=%d  parent=%s",
            recording_id, ad_id, len(all_hashes),
            best_parent_id or "none",
            extra=_extra(PROC)
        )
        return {"ok": True, "ad_id": ad_id, "suggested_parent": best_parent_id}

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
    backend     = state_get("mute_backend", "cec")

    with get_conn() as conn:
        vault_size = conn.execute(
            "SELECT COUNT(*) FROM ads WHERE is_active=1"
        ).fetchone()[0]

    return web.json_response({
        "is_muted":        mute_status.get("is_muted",        False),
        "current_ad_name": mute_status.get("current_ad_name"),
        "current_ad_id":   mute_status.get("current_ad_id"),
        "mic_active":      mic_active,
        "mute_backend":    backend,
        "vault_size":      vault_size,
        "snr_db":          float(last_snr) if last_snr  else None,
        "snr_class":       last_class,
        "uptime":          round(time.time() - request.app["start_time"], 1),
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
        "cache": {
            "l1_size":           cfg.cache.l1_size,
            "l2_size":           cfg.cache.l2_size,
            "heartbeat_seconds": cfg.cache.heartbeat_seconds,
        },
        "mute": {
            "backend":               cfg.mute.backend,
            "safety_margin_seconds": cfg.mute.safety_margin_seconds,
            "cec_device_name":       cfg.mute.cec_device_name,
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
    log    = request.app["log"]

    if not result.get("ok"):
        log.error("SNR test failed: %s", result, extra=_extra(PROC))
    else:
        log.info(
            "SNR test: %.1f dB [%s]",
            result.get("snr_db", 0), result.get("classification", "?"),
            extra=_extra(PROC)
        )

    if result.get("ok"):
        state_set("last_snr_db",    str(result["snr_db"]))
        state_set("last_snr_class", result["classification"])
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO snr_log (snr_db, classification, placement_note) "
                "VALUES (?,?,?)",
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
            "INSERT INTO recordings (file_path, duration_seconds, status) "
            "VALUES (?,?,?)",
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
                      a.name AS parent_name
               FROM   recordings r
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


# ── Workbench ─────────────────────────────────────────────────────────────────

async def summon_recordings(request: web.Request) -> web.Response:
    """
    Fetch specific recordings by a comma-separated list of IDs.
    Powers the Workbench review panel when the user brings up a
    specific set of recordings for side-by-side inspection.
    """
    id_str = request.rel_url.query.get("ids", "")
    if not id_str:
        return web.json_response([])

    try:
        ids = [int(x.strip()) for x in id_str.split(",") if x.strip().isdigit()]
    except ValueError:
        return web.json_response({"error": "Invalid ID format"}, status=400)

    if not ids:
        return web.json_response([])

    with get_conn() as conn:
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"""SELECT r.*, a.name AS parent_name
                FROM   recordings r
                LEFT JOIN ads a ON r.pending_link_ad_id = a.id
                WHERE  r.id IN ({placeholders})
                ORDER BY r.id ASC""",
            ids
        ).fetchall()

    return web.json_response([dict(r) for r in rows])


async def manual_link(request: web.Request) -> web.Response:
    """
    Manually establish a parent-child relationship between two vault entries.
    Used by the Workbench when the user wants to link a recording to an
    existing ad without going through the automatic variant suggestion flow.
    """
    data      = await _json(request)
    child_id  = data.get("child_id")
    parent_id = data.get("parent_id")

    if not child_id or not parent_id:
        return web.json_response(
            {"error": "Missing child_id or parent_id"}, status=400
        )

    with get_conn() as conn:
        conn.execute(
            "UPDATE recordings SET pending_link_ad_id=?, status='ingested' "
            "WHERE id=?",
            (parent_id, child_id)
        )
        conn.execute(
            "UPDATE ads SET parent_ad_id=?, is_active=1 "
            "WHERE id = (SELECT ad_id FROM recordings WHERE id=?)",
            (parent_id, child_id)
        )
        conn.commit()

    return web.json_response({
        "ok":      True,
        "message": f"Linked #{child_id} as child of #{parent_id}",
    })


async def get_recording_audio(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute(
            "SELECT file_path FROM recordings WHERE id=?", (rec_id,)
        ).fetchone()
    if not row:
        return web.json_response({"error": "Not found"}, status=404)
    filepath = Path(row["file_path"])
    if not filepath.exists():
        return web.json_response({"error": "File missing"}, status=404)
    return web.FileResponse(filepath, headers={
        "Content-Type":        "audio/wav",
        "Content-Disposition": f"inline; filename={filepath.name}",
    })


async def update_recording(request: web.Request) -> web.Response:
    rec_id  = int(request.match_info["id"])
    data    = await _json(request)
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
    result = await loop.run_in_executor(
        None, _ingest_recording, rec_id, cfg, log
    )
    return web.json_response(result, status=200 if result["ok"] else 422)


async def review_accept(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        conn.execute(
            "UPDATE recordings SET status='ingested' WHERE id=?", (rec_id,)
        )
        conn.execute(
            "UPDATE ads SET is_active=1 "
            "WHERE id = (SELECT ad_id FROM recordings WHERE id=?)",
            (rec_id,)
        )
        conn.commit()
    return web.json_response({"ok": True})


async def review_reject(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        conn.execute(
            "UPDATE recordings SET status='ingested', pending_link_ad_id=NULL "
            "WHERE id=?",
            (rec_id,)
        )
        conn.execute(
            "UPDATE ads SET is_active=1, parent_ad_id=NULL "
            "WHERE id = (SELECT ad_id FROM recordings WHERE id=?)",
            (rec_id,)
        )
        conn.commit()
    return web.json_response({"ok": True})


async def delete_recording(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM recordings WHERE id=?", (rec_id,)
        ).fetchone()
    if not row:
        return web.json_response({"error": "Not found"}, status=404)

    fp = Path(row["file_path"])
    if fp.exists():
        fp.unlink()

    with get_conn() as conn:
        if row["ad_id"]:
            conn.execute("DELETE FROM hashes WHERE ad_id=?",  (row["ad_id"],))
            conn.execute("DELETE FROM ads    WHERE id=?",     (row["ad_id"],))
        conn.execute("DELETE FROM recordings WHERE id=?", (rec_id,))
        conn.commit()

    return web.json_response({"ok": True})


# ── Vault ─────────────────────────────────────────────────────────────────────

async def list_ads(request: web.Request) -> web.Response:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, name, duration_seconds, streaming_service,
                      category, is_active, hash_count, source,
                      parent_ad_id, created_at
               FROM   ads
               ORDER BY created_at DESC"""
        ).fetchall()
    return web.json_response([{
        "id":                r["id"],
        "name":              r["name"],
        "duration_seconds":  r["duration_seconds"],
        "streaming_service": r["streaming_service"],
        "category":          r["category"],
        "is_active":         bool(r["is_active"]),
        "hash_count":        r["hash_count"],
        "source":            r["source"],
        "parent_ad_id":      r["parent_ad_id"],
        "created_at":        r["created_at"],
    } for r in rows])


async def update_ad(request: web.Request) -> web.Response:
    ad_id   = int(request.match_info["id"])
    data    = await _json(request)
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


async def reactivate_ad(request: web.Request) -> web.Response:
    ad_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM ads WHERE id=?", (ad_id,)
        ).fetchone()
        if not row:
            return web.json_response({"error": "Ad not found"}, status=404)
        conn.execute("UPDATE ads SET is_active=1 WHERE id=?", (ad_id,))
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
            ad = conn.execute(
                "SELECT * FROM ads WHERE id=?", (ad_id,)
            ).fetchone()
            if not ad:
                return web.json_response({"error": "Ad not found"}, status=404)
            hash_count = conn.execute(
                "SELECT COUNT(*) FROM hashes WHERE ad_id=?", (ad_id,)
            ).fetchone()[0]
            mute_count = conn.execute(
                "SELECT COUNT(*) FROM mute_log WHERE ad_id=?", (ad_id,)
            ).fetchone()[0]
        return web.json_response({
            "ok":               False,
            "requires_confirm": True,
            "ad_name":          ad["name"],
            "impact": {
                "hashes_to_delete":    hash_count,
                "mute_history_entries": mute_count,
                "warning": (
                    f"This will permanently delete \"{ad['name']}\" and its "
                    f"{hash_count} fingerprint hashes. "
                    "It will no longer be detected. This cannot be undone."
                ),
            },
        })

    with get_conn() as conn:
        conn.execute("DELETE FROM ads WHERE id=?", (ad_id,))
        conn.commit()

    request.app["log"].info(
        "Purged ad_id=%d and all its hashes", ad_id, extra=_extra(PROC)
    )
    return web.json_response({"ok": True, "purged": True})


async def get_ad_audio(request: web.Request) -> web.Response:
    ad_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute(
            "SELECT r.file_path FROM recordings r "
            "WHERE r.ad_id=? AND r.status='ingested' LIMIT 1",
            (ad_id,)
        ).fetchone()
    if not row:
        return web.json_response({"error": "No audio file for this ad"}, status=404)
    fp = Path(row["file_path"])
    if not fp.exists():
        return web.json_response({"error": "File missing"}, status=404)
    return web.FileResponse(fp, headers={"Content-Type": "audio/wav"})


# ── Stats ─────────────────────────────────────────────────────────────────────

async def stats_summary(request: web.Request) -> web.Response:
    with get_conn() as conn:
        secs = conn.execute(
            "SELECT COALESCE(SUM(duration_actual), 0) FROM mute_log "
            "WHERE muted_at >= date('now','start of month') "
            "AND was_false_positive = 0"
        ).fetchone()[0]
        mute_count = conn.execute(
            "SELECT COUNT(*) FROM mute_log "
            "WHERE muted_at >= date('now','start of month') "
            "AND was_false_positive = 0"
        ).fetchone()[0]
        fp_count = conn.execute(
            "SELECT COUNT(*) FROM mute_log "
            "WHERE muted_at >= date('now','start of month') "
            "AND was_false_positive = 1"
        ).fetchone()[0]
        vault_size = conn.execute(
            "SELECT COUNT(*) FROM ads WHERE is_active=1"
        ).fetchone()[0]
        top_ads = conn.execute(
            """SELECT m.ad_id,
                      m.ad_name_snapshot,
                      COUNT(*)          AS cnt,
                      a.category,
                      a.streaming_service,
                      (SELECT notes FROM recordings
                       WHERE ad_id = a.id LIMIT 1) AS notes
               FROM   mute_log m
               LEFT JOIN ads a ON m.ad_id = a.id
               WHERE  m.muted_at >= date('now','start of month')
                 AND  m.was_false_positive = 0
                 AND  m.ad_name_snapshot IS NOT NULL
               GROUP BY m.ad_name_snapshot, m.ad_id,
                        a.category, a.streaming_service
               ORDER BY cnt DESC
               LIMIT 5"""
        ).fetchall()

    return web.json_response({
        "minutes_saved_this_month": round(secs / 60.0, 1),
        "mute_events_this_month":   mute_count,
        "false_positives_this_month": fp_count,
        "vault_size":               vault_size,
        "top_muted_ads": [{
            "id":                r["ad_id"],
            "name":              r["ad_name_snapshot"],
            "category":          r["category"],
            "streaming_service": r["streaming_service"],
            "notes":             r["notes"],
            "count":             r["cnt"],
        } for r in top_ads],
    })


async def stats_mute_log(request: web.Request) -> web.Response:
    page    = int(request.rel_url.query.get("page",  1))
    limit   = int(request.rel_url.query.get("limit", 20))
    fp_only = request.rel_url.query.get("fp_only", "false") == "true"
    offset  = (page - 1) * limit
    where   = "WHERE was_false_positive=1" if fp_only else ""

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM mute_log {where}"
        ).fetchone()[0]
        rows = conn.execute(
            f"""SELECT m.id, m.ad_id, m.ad_name_snapshot, m.muted_at,
                       m.unmuted_at, m.duration_actual, m.mute_method,
                       m.confidence_score, m.was_false_positive,
                       a.category, a.streaming_service,
                       (SELECT notes FROM recordings
                        WHERE ad_id = a.id LIMIT 1) AS notes
                FROM   mute_log m
                LEFT JOIN ads a ON m.ad_id = a.id
                {where}
                ORDER BY m.muted_at DESC
                LIMIT ? OFFSET ?""",
            (limit, offset)
        ).fetchall()

    return web.json_response({
        "total": total,
        "page":  page,
        "limit": limit,
        "items": [{
            "id":               r["id"],
            "ad_id":            r["ad_id"],
            "ad_name":          r["ad_name_snapshot"],
            "category":         r["category"],
            "streaming_service":r["streaming_service"],
            "notes":            r["notes"],
            "muted_at":         r["muted_at"],
            "unmuted_at":       r["unmuted_at"],
            "duration_actual":  r["duration_actual"],
            "mute_method":      r["mute_method"],
            "confidence_score": r["confidence_score"],
            "was_false_positive": bool(r["was_false_positive"]),
        } for r in rows],
    })


async def stats_snr_history(request: web.Request) -> web.Response:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT snr_db, classification, placement_note, measured_at "
            "FROM snr_log ORDER BY measured_at DESC LIMIT 30"
        ).fetchall()
    return web.json_response([{
        "snr_db":         r["snr_db"],
        "classification": r["classification"],
        "note":           r["placement_note"],
        "measured_at":    r["measured_at"],
    } for r in rows])


# ── FRONTEND ──────────────────────────────────────────────────────────────────

async def serve_index(request: web.Request) -> web.Response:
    return web.FileResponse(BASE / "static" / "index.html")


# ── APP FACTORY ───────────────────────────────────────────────────────────────

_ALL_ROUTES = [
    "/api/health", "/api/status", "/api/config",
    "/api/mute/toggle", "/api/mute/false-positive",
    "/api/mic/toggle", "/api/snr-test",
    "/api/record/start", "/api/record/stop",
    "/api/recordings", "/api/recordings/summon",
    "/api/recordings/{id}", "/api/recordings/{id}/audio",
    "/api/recordings/{id}/ingest",
    "/api/recordings/{id}/link/accept",
    "/api/recordings/{id}/link/reject",
    "/api/ads", "/api/ads/link",
    "/api/ads/{id}", "/api/ads/{id}/audio",
    "/api/ads/{id}/reactivate", "/api/ads/{id}/deactivate",
    "/api/ads/{id}/purge",
    "/api/stats/summary", "/api/stats/mute-log",
    "/api/stats/snr-history",
]


def create_app(cfg, log: logging.Logger, secret: str) -> web.Application:
    app = web.Application(
        middlewares=[cors_middleware, auth_middleware(secret)]
    )
    app["cfg"]        = cfg
    app["log"]        = log
    app["start_time"] = time.time()
    app["secret"]     = secret

    static_dir = BASE / "static"
    static_dir.mkdir(exist_ok=True)

    # ── Routes ────────────────────────────────────────────────────────────────
    app.router.add_get   ("/",                                  serve_index)
    app.router.add_get   ("/api/health",                        health)
    app.router.add_get   ("/api/status",                        get_status)
    app.router.add_get   ("/api/config",                        get_config)

    app.router.add_post  ("/api/mute/toggle",                   mute_toggle)
    app.router.add_post  ("/api/mute/false-positive",           false_positive)

    app.router.add_post  ("/api/mic/toggle",                    mic_toggle)
    app.router.add_post  ("/api/snr-test",                      snr_test)

    app.router.add_post  ("/api/record/start",                  record_start)
    app.router.add_post  ("/api/record/stop",                   record_stop)

    app.router.add_get   ("/api/recordings",                    list_recordings)
    app.router.add_get   ("/api/recordings/summon",             summon_recordings)
    app.router.add_get   ("/api/recordings/{id}/audio",         get_recording_audio)
    app.router.add_route ("PATCH",  "/api/recordings/{id}",     update_recording)
    app.router.add_post  ("/api/recordings/{id}/ingest",        ingest_recording)
    app.router.add_post  ("/api/recordings/{id}/link/accept",   review_accept)
    app.router.add_post  ("/api/recordings/{id}/link/reject",   review_reject)
    app.router.add_route ("DELETE", "/api/recordings/{id}",     delete_recording)

    app.router.add_get   ("/api/ads",                           list_ads)
    app.router.add_post  ("/api/ads/link",                      manual_link)
    app.router.add_route ("PATCH",  "/api/ads/{id}",            update_ad)
    app.router.add_post  ("/api/ads/{id}/reactivate",           reactivate_ad)
    app.router.add_post  ("/api/ads/{id}/deactivate",           deactivate_ad)
    app.router.add_post  ("/api/ads/{id}/purge",                purge_ad)
    app.router.add_get   ("/api/ads/{id}/audio",                get_ad_audio)

    app.router.add_get   ("/api/stats/summary",                 stats_summary)
    app.router.add_get   ("/api/stats/mute-log",                stats_mute_log)
    app.router.add_get   ("/api/stats/snr-history",             stats_snr_history)

    # Preflight OPTIONS for every API route
    for path in _ALL_ROUTES:
        app.router.add_route("OPTIONS", path,
                             lambda r: web.Response(status=200))

    app.router.add_static("/static", static_dir,
                          name="static", show_index=False)
    return app


# ── PROCESS ENTRY POINT ───────────────────────────────────────────────────────

def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    cfg    = load_config()
    log    = setup_logging(PROC, log_level, log_dir)
    secret = _get_or_create_secret()
    app    = create_app(cfg, log, secret)
    log.info("Starting on port %d", cfg.api.port, extra=_extra(PROC))
    web.run_app(
        app,
        host       = "0.0.0.0",
        port       = cfg.api.port,
        print      = None,
        access_log = None,
    )


if __name__ == "__main__":
    run()