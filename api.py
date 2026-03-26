"""
AdMute v4 — Local API
aiohttp server on port 5000.
HMAC-SHA256 authentication on all endpoints except /api/health.

On first run, a secret is auto-generated and printed to the terminal.
Copy it into the web UI setup screen.
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

PROC = "API"


# ── SECRET MANAGEMENT ─────────────────────────────────────────────────────────

def _get_or_create_secret() -> str:
    """
    Load the HMAC secret from device_state.
    If it doesn't exist yet, generate one and print it to the terminal.
    """
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
    """
    Verify HMAC-SHA256 Authorization header.
    Format: HMAC <timestamp>.<signature>
    Signature = HMAC-SHA256(secret, timestamp + method + path + body)
    Timestamp must be within 60 seconds of server time.
    """
    try:
        scheme, token     = auth_header.split(" ", 1)
        if scheme.upper() != "HMAC":
            return False
        timestamp_str, sig = token.split(".", 1)
        timestamp          = int(timestamp_str)

        # Replay protection — reject requests older than 120s
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
    """
    Allow requests from any origin on the local network.
    Required for the phone browser to talk to the Pi API.
    """
    # Handle preflight OPTIONS requests
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
        # Health check is always open
        if request.path == "/api/health":
            return await handler(request)

        if "/audio" in request.path:
            return await handler(request)

        # Static files are open (the UI handles its own auth)
        if not request.path.startswith("/api/"):
            return await handler(request)

        auth = request.headers.get("Authorization", "")
        body = await request.read()

        if not _verify_hmac(request.method, request.path_qs,
                             body, auth, secret):
            return web.json_response(
                {"error": "Unauthorized"}, status=401
            )

        # Re-attach body so handlers can read it
        request._stored_body = body
        return await handler(request)

    return _middleware


async def _body(request: web.Request) -> bytes:
    """Get request body — handles pre-read by auth middleware."""
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
    """
    Send a REQ to a ZMQ REP server and return the response.
    Returns {"ok": False, "error": "..."} on timeout or failure.
    Synchronous — called from a thread pool executor.
    """
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


# ── FINGERPRINT INGESTION ─────────────────────────────────────────────────────

def _ingest_recording(recording_id: int,
                      cfg, log: logging.Logger) -> dict:
    """
    Load a WAV recording, run the full fingerprint pipeline,
    and insert the ad + hashes into the vault.
    Returns a result dict.
    """
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

    # Load WAV
    try:
        with wave.open(str(filepath), 'rb') as wf:
            frames    = wf.readframes(wf.getnframes())
            framerate = wf.getframerate()

        audio_int = np.frombuffer(frames, dtype=np.int32)
        audio     = audio_int.astype(np.float32) / 2_147_483_647.0
        duration  = len(audio) / framerate

    except Exception as e:
        return {"ok": False, "error": f"Failed to read WAV: {e}"}

    # Fingerprint using the SAME rolling window approach as live capture
    # This ensures stored hashes are directly comparable to live hashes
    _init_filter(cfg.audio.sample_rate)

    chunk_size     = cfg.audio.chunk_size        # 2048
    window_samples = cfg.audio.sample_rate       # 48000 = 1 second
    hop_samples    = window_samples // 2         # 50% overlap

    all_hashes: list[tuple[int, int]] = []
    start = 0

    while start + window_samples <= len(audio):
        window = audio[start:start + window_samples].copy()

        # Apply same pipeline as live: filter then normalise
        filtered   = _apply_filter(window)
        normalised = _normalise_peak(filtered, 0.0005)

        if normalised is not None:
            window_hashes = generate_hashes(normalised, cfg.fingerprint,
                                            cfg.audio.sample_rate)
            all_hashes.extend(window_hashes)

        start += hop_samples

    hashes = all_hashes

    if len(hashes) < 50:
        return {"ok": False,
                "error": f"Only {len(hashes)} hashes generated — "
                         f"too few to match reliably. Re-record."}

    # Write to DB
    try:
        with get_conn() as conn:
            # Create the ad record
            name = (row["streaming_service"] or "Unknown") + \
                   " Ad " + str(int(time.time()))[-4:]

            conn.execute(
                """INSERT INTO ads
                   (name, duration_seconds, streaming_service,
                    category, source, hash_count)
                   VALUES (?,?,?,?,?,?)""",
                (name, round(duration, 2),
                 row["streaming_service"] or "Unknown",
                 row["category"]          or "Uncategorized",
                 "mic",
                 len(hashes))
            )
            ad_id = conn.execute(
                "SELECT last_insert_rowid()"
            ).fetchone()[0]

            # Bulk insert hashes
            conn.executemany(
                "INSERT INTO hashes (hash_value, time_offset, ad_id) "
                "VALUES (?,?,?)",
                [(h, off, ad_id) for h, off in hashes]
            )

            # Update recording status
            conn.execute(
                "UPDATE recordings SET status='ingested', ad_id=? "
                "WHERE id=?",
                (ad_id, recording_id)
            )
            conn.commit()

        log.info("Ingested recording %d → ad_id=%d  hashes=%d  "
                 "duration=%.1fs",
                 recording_id, ad_id, len(hashes), duration,
                 extra=_extra(PROC))

        return {
            "ok":         True,
            "ad_id":      ad_id,
            "hash_count": len(hashes),
            "duration":   round(duration, 2),
            "name":       name,
        }

    except sqlite3.Error as e:
        return {"ok": False, "error": f"DB error: {e}"}


# ── ROUTE HANDLERS ────────────────────────────────────────────────────────────

# ── SYSTEM ────────────────────────────────────────────────────

async def health(request: web.Request) -> web.Response:
    """Unauthenticated liveness check."""
    return web.json_response({
        "status": "ok",
        "uptime": round(time.time() - request.app["start_time"], 1),
    })


async def get_status(request: web.Request) -> web.Response:
    """Full device status — mute state, mic, backend, last SNR."""
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
    """Return current config with secrets redacted."""
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


# ── MUTE CONTROL ──────────────────────────────────────────────

async def mute_toggle(request: web.Request) -> web.Response:
    result = await _cmd_mute({"cmd": "mute_toggle"})
    return web.json_response(result)


async def false_positive(request: web.Request) -> web.Response:
    result = await _cmd_mute({"cmd": "false_positive"})
    return web.json_response(result)


# ── MIC CONTROL ───────────────────────────────────────────────

async def mic_toggle(request: web.Request) -> web.Response:
    result = await _cmd_audio({"cmd": "mic_toggle"})
    if result.get("ok"):
        state_set("mic_active", "1" if result["mic_active"] else "0")
    return web.json_response(result)


# ── SNR TEST ──────────────────────────────────────────────────

async def snr_test(request: web.Request) -> web.Response:
    result = await _cmd_audio({"cmd": "snr_test"})
    
    # Log the raw result so we can debug from the terminal
    log = request.app["log"]
    if not result.get("ok"):
        log.error("SNR test ZMQ call failed: %s", result,
                  extra=_extra(PROC))
    else:
        log.info("SNR test: %.1fdB [%s]",
                 result.get("snr_db", 0),
                 result.get("classification", "?"),
                 extra=_extra(PROC))
    
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

# ── RECORDING ─────────────────────────────────────────────────

async def record_start(request: web.Request) -> web.Response:
    result = await _cmd_audio({"cmd": "start_record"})
    return web.json_response(result)


async def record_stop(request: web.Request) -> web.Response:
    result = await _cmd_audio({"cmd": "stop_record"})
    if not result.get("ok"):
        return web.json_response(result, status=500)

    filepath = result["filepath"]
    duration = result["duration"]

    # Create recordings DB entry
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO recordings
               (file_path, duration_seconds, status)
               VALUES (?,?,?)""",
            (filepath, duration, "pending_review")
        )
        rec_id = conn.execute(
            "SELECT last_insert_rowid()"
        ).fetchone()[0]
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
            """SELECT id, ad_id, file_path, duration_seconds,
                      status, streaming_service, category,
                      notes, recorded_at
               FROM recordings
               ORDER BY recorded_at DESC"""
        ).fetchall()

    return web.json_response([{
        "id":                r["id"],
        "ad_id":             r["ad_id"],
        "filename":          Path(r["file_path"]).name,
        "duration_seconds":  r["duration_seconds"],
        "status":            r["status"],
        "streaming_service": r["streaming_service"],
        "category":          r["category"],
        "notes":             r["notes"],
        "recorded_at":       r["recorded_at"],
    } for r in rows])


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

    return web.FileResponse(
        filepath,
        headers={"Content-Type": "audio/wav",
                 "Content-Disposition": f"inline; filename={filepath.name}"}
    )


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
        conn.execute(
            f"UPDATE recordings SET {set_clause} WHERE id=?", values
        )
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

    status = 200 if result["ok"] else 422
    return web.json_response(result, status=status)


async def delete_recording(request: web.Request) -> web.Response:
    rec_id = int(request.match_info["id"])

    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM recordings WHERE id=?", (rec_id,)
        ).fetchone()

    if not row:
        return web.json_response({"error": "Not found"}, status=404)


    # Delete WAV file
    fp = Path(row["file_path"])
    if fp.exists():
        fp.unlink()

    with get_conn() as conn:
        conn.execute("DELETE FROM recordings WHERE id=?", (rec_id,))
        conn.commit()

    return web.json_response({"ok": True})


# ── AD VAULT ──────────────────────────────────────────────────

async def list_ads(request: web.Request) -> web.Response:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, name, duration_seconds, streaming_service,
                      category, is_active, hash_count, source, created_at
               FROM ads
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
        "created_at":        r["created_at"],
    } for r in rows])


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
        conn.execute(
            f"UPDATE ads SET {set_clause} WHERE id=?", values
        )
        conn.commit()

    return web.json_response({"ok": True})


async def deactivate_ad(request: web.Request) -> web.Response:
    """Soft delete — marks inactive but keeps hashes. Reversible."""
    ad_id = int(request.match_info["id"])
    with get_conn() as conn:
        conn.execute(
            "UPDATE ads SET is_active=0 WHERE id=?", (ad_id,)
        )
        conn.commit()
    return web.json_response({"ok": True})


async def purge_ad(request: web.Request) -> web.Response:
    """
    Hard delete — removes ad AND all its hashes permanently.
    Requires {"confirm": true} in the request body.
    This is the two-step delete — the UI must ask the user to confirm.
    """
    ad_id = int(request.match_info["id"])
    data  = await _json(request)

    if not data.get("confirm"):
        # Step 1 — return impact summary without deleting anything
        with get_conn() as conn:
            ad = conn.execute(
                "SELECT * FROM ads WHERE id=?", (ad_id,)
            ).fetchone()
            if not ad:
                return web.json_response(
                    {"error": "Ad not found"}, status=404
                )
            hash_count  = conn.execute(
                "SELECT COUNT(*) FROM hashes WHERE ad_id=?", (ad_id,)
            ).fetchone()[0]
            mute_count  = conn.execute(
                "SELECT COUNT(*) FROM mute_log WHERE ad_id=?", (ad_id,)
            ).fetchone()[0]

        return web.json_response({
            "ok":         False,
            "requires_confirm": True,
            "ad_name":    ad["name"],
            "impact": {
                "hashes_to_delete":       hash_count,
                "mute_history_entries":   mute_count,
                "warning": (
                    f"This will permanently delete \"{ad['name']}\" "
                    f"and its {hash_count:,} fingerprint hashes. "
                    f"It will no longer be detected. "
                    f"This cannot be undone."
                ),
            },
        })

    # Step 2 — confirmed, execute the purge
    with get_conn() as conn:
        # Hashes are deleted by CASCADE (foreign key ON DELETE CASCADE)
        conn.execute("DELETE FROM ads WHERE id=?", (ad_id,))
        conn.commit()

    request.app["log"].info(
        "Purged ad_id=%d and all its hashes", ad_id,
        extra=_extra(PROC)
    )
    return web.json_response({"ok": True, "purged": True})


async def get_ad_audio(request: web.Request) -> web.Response:
    ad_id = int(request.match_info["id"])
    with get_conn() as conn:
        row = conn.execute(
            """SELECT r.file_path FROM recordings r
               WHERE r.ad_id=? AND r.status='ingested'
               LIMIT 1""",
            (ad_id,)
        ).fetchone()

    if not row:
        return web.json_response(
            {"error": "No audio file for this ad"}, status=404
        )

    fp = Path(row["file_path"])
    if not fp.exists():
        return web.json_response({"error": "File missing"}, status=404)

    return web.FileResponse(fp,
        headers={"Content-Type": "audio/wav"})


# ── STATS ─────────────────────────────────────────────────────

async def stats_summary(request: web.Request) -> web.Response:
    with get_conn() as conn:
        # Minutes saved this month
        secs = conn.execute(
            """SELECT COALESCE(SUM(duration_actual), 0)
               FROM mute_log
               WHERE muted_at >= date('now','start of month')
               AND was_false_positive = 0"""
        ).fetchone()[0]

        # Total mute events this month
        mute_count = conn.execute(
            """SELECT COUNT(*) FROM mute_log
               WHERE muted_at >= date('now','start of month')
               AND was_false_positive = 0"""
        ).fetchone()[0]

        # False positives this month
        fp_count = conn.execute(
            """SELECT COUNT(*) FROM mute_log
               WHERE muted_at >= date('now','start of month')
               AND was_false_positive = 1"""
        ).fetchone()[0]

        # Total ads in vault
        vault_size = conn.execute(
            "SELECT COUNT(*) FROM ads WHERE is_active=1"
        ).fetchone()[0]

        # Top 5 most muted ads this month
        top_ads = conn.execute(
            """SELECT m.ad_id, m.ad_name_snapshot, COUNT(*) as cnt,
                      a.category, a.streaming_service,
                      (SELECT notes FROM recordings WHERE ad_id = a.id LIMIT 1) as notes
               FROM mute_log m
               LEFT JOIN ads a ON m.ad_id = a.id
               WHERE m.muted_at >= date('now','start of month')
               AND m.was_false_positive = 0
               AND m.ad_name_snapshot IS NOT NULL
               GROUP BY m.ad_name_snapshot, m.ad_id, a.category, a.streaming_service
               ORDER BY cnt DESC
               LIMIT 5"""
        ).fetchall()

    return web.json_response({
        "minutes_saved_this_month": round(secs / 60.0, 1),
        "mute_events_this_month":   mute_count,
        "false_positives_this_month": fp_count,
        "vault_size":               vault_size,
        "top_muted_ads": [
            {
                "id": r["ad_id"],
                "name": r["ad_name_snapshot"],
                "category": r["category"],
                "streaming_service": r["streaming_service"],
                "notes": r["notes"],
                "count": r["cnt"]
            }
            for r in top_ads
        ],
    })

    return web.json_response({
        "minutes_saved_this_month": round(secs / 60.0, 1),
        "mute_events_this_month":   mute_count,
        "false_positives_this_month": fp_count,
        "vault_size":               vault_size,
        "top_muted_ads": [
            {"name": r["ad_name_snapshot"], "count": r["cnt"]}
            for r in top_ads
        ],
    })


async def stats_mute_log(request: web.Request) -> web.Response:
    page  = int(request.rel_url.query.get("page",  1))
    limit = int(request.rel_url.query.get("limit", 20))
    fp_only = request.rel_url.query.get("fp_only", "false") == "true"
    offset  = (page - 1) * limit

    where = "WHERE was_false_positive=1" if fp_only else ""

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM mute_log {where}"
        ).fetchone()[0]
        rows = conn.execute(
            f"""SELECT m.id, m.ad_id, m.ad_name_snapshot, m.muted_at,
                       m.unmuted_at, m.duration_actual, m.mute_method,
                       m.confidence_score, m.was_false_positive,
                       a.category, a.streaming_service,
                       (SELECT notes FROM recordings WHERE ad_id = a.id LIMIT 1) as notes
                FROM mute_log m
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
            """SELECT snr_db, classification, placement_note, measured_at
               FROM snr_log
               ORDER BY measured_at DESC
               LIMIT 30"""
        ).fetchall()

    return web.json_response([{
        "snr_db":         r["snr_db"],
        "classification": r["classification"],
        "note":           r["placement_note"],
        "measured_at":    r["measured_at"],
    } for r in rows])



async def serve_index(request: web.Request) -> web.Response:
    index = BASE / "static" / "index.html"
    return web.FileResponse(index)


def create_app(cfg, log: logging.Logger,
               secret: str) -> web.Application:

    app = web.Application(middlewares=[cors_middleware, auth_middleware(secret)])
    app["cfg"]        = cfg
    app["log"]        = log
    app["start_time"] = time.time()
    app["secret"]     = secret

    static_dir = BASE / "static"
    static_dir.mkdir(exist_ok=True)

    app.router.add_get ("/",                              serve_index)
    app.router.add_get ("/api/health",                    health)
    app.router.add_get ("/api/status",                    get_status)
    app.router.add_get ("/api/config",                    get_config)

    app.router.add_post("/api/mute/toggle",               mute_toggle)
    app.router.add_post("/api/mute/false-positive",       false_positive)
    app.router.add_post("/api/mic/toggle",                mic_toggle)
    app.router.add_post("/api/snr-test",                  snr_test)

    app.router.add_post("/api/record/start",              record_start)
    app.router.add_post("/api/record/stop",               record_stop)
    app.router.add_get ("/api/recordings",                list_recordings)
    app.router.add_get ("/api/recordings/{id}/audio",     get_recording_audio)
    app.router.add_route("PATCH",  "/api/recordings/{id}", update_recording)
    app.router.add_post("/api/recordings/{id}/ingest",    ingest_recording)
    app.router.add_route("DELETE", "/api/recordings/{id}", delete_recording)

    app.router.add_get ("/api/ads",                       list_ads)
    app.router.add_route("PATCH",  "/api/ads/{id}",       update_ad)
    app.router.add_post("/api/ads/{id}/deactivate",       deactivate_ad)
    app.router.add_post("/api/ads/{id}/purge",            purge_ad)
    app.router.add_get ("/api/ads/{id}/audio",            get_ad_audio)

    app.router.add_get ("/api/stats/summary",             stats_summary)
    app.router.add_get ("/api/stats/mute-log",            stats_mute_log)
    app.router.add_get ("/api/stats/snr-history",         stats_snr_history)

    for path in [
        "/api/health", "/api/status", "/api/config",
        "/api/mute/toggle", "/api/mute/false-positive",
        "/api/mic/toggle", "/api/snr-test",
        "/api/record/start", "/api/record/stop",
        "/api/recordings", "/api/recordings/{id}",
        "/api/recordings/{id}/audio", "/api/recordings/{id}/ingest",
        "/api/ads", "/api/ads/{id}",
        "/api/ads/{id}/deactivate", "/api/ads/{id}/purge",
        "/api/ads/{id}/audio",
        "/api/stats/summary", "/api/stats/mute-log",
        "/api/stats/snr-history",
    ]:
        app.router.add_route("OPTIONS", path,
                             lambda r: web.Response(status=200))

    app.router.add_static("/static", static_dir,
                          name="static", show_index=False)

    return app

# ── APP FACTORY ───────────────────────────────────────────────────────────────

# ── PROCESS ENTRY POINT ───────────────────────────────────────────────────────

def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    cfg    = load_config()
    log    = setup_logging(PROC, log_level, log_dir)
    secret = _get_or_create_secret()

    app    = create_app(cfg, log, secret)

    log.info("Starting on port %d", cfg.api.port, extra=_extra(PROC))

    web.run_app(
        app,
        host   = "0.0.0.0",
        port   = cfg.api.port,
        print  = None,          # suppress aiohttp's own startup banner
        access_log = None,      # use our own logging
    )


if __name__ == "__main__":
    run()
