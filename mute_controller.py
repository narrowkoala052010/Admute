"""
AdMute v6 — Process 4: MuteController
Calculates absolute unmute timing using STFT delta offsets.
Listens for Match Engine ZMQ broadcasts and executes IR/CEC.
"""

import os
import sys
import time
import json
import signal
import sqlite3
import threading
import subprocess
import zmq
from pathlib import Path

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config  import load_config
from db      import get_conn, state_get, state_set
from bus     import MATCH_PUB, MUTE_CTRL, TOPIC_MATCH, TOPIC_NEAR_MISS, TOPIC_STRIKE
from console import (setup_logging, _extra,
                     fmt_mute_sent, fmt_unmute_sent, fmt_false_positive)

PROC = "MUTE"


class MuteController:
    """
    Stateful mute manager. Thread-safe.
    All state transitions go through this class.
    """

    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log

        self._lock            = threading.Lock()
        self._muted           = False
        self._current_ad_id   = None
        self._current_ad_name = ""
        self._mute_start      = 0.0
        self._unmute_timer: threading.Timer | None = None
        self._last_match_ts   = 0.0

        # Pre-calculate the exact duration of a single STFT frame in seconds
        hop_samples = self.cfg.fingerprint.nperseg * (1.0 - self.cfg.fingerprint.noverlap_ratio)
        self.stft_frame_duration = hop_samples / float(self.cfg.audio.sample_rate)

    # ── PUBLIC API ─────────────────────────────────────────────

    @property
    def is_muted(self) -> bool:
        with self._lock:
            return self._muted

    @property
    def current_ad(self) -> dict | None:
        with self._lock:
            if not self._muted:
                return None
            return {
                "ad_id":    self._current_ad_id,
                "ad_name":  self._current_ad_name,
                "muted_at": self._mute_start,
            }

    def on_match(self, ad_id: int, ad_name: str,
                 duration: float, delta: int, score: int, trace_id: str) -> None:
        """Called when MatchEngine fires a confident match."""
        with self._lock:
            now = time.time()

            # Debounce — ignore if we just acted on a match
            if now - self._last_match_ts < self.cfg.mute.debounce_ms / 1000.0:
                return

            # Same ad is already playing — ignore
            if self._muted and self._current_ad_id == ad_id:
                return

            # Different ad interrupted — cancel existing timer
            if self._muted:
                self._cancel_timer()
                self._do_unmute_command(reason="interrupted by new ad", trace_id=trace_id)

            self._last_match_ts   = now
            self._current_ad_id   = ad_id
            self._current_ad_name = ad_name
            self._mute_start      = now

            self._do_mute_command(trace_id=trace_id)
            self._muted = True

            # ── THE V6 ABSOLUTE TIMELINE MATH ──
            # Calculate exactly how many seconds into the commercial we are
            elapsed_seconds = delta * self.stft_frame_duration
            
            # The remaining time is the total duration minus the elapsed time and safety margin
            calculated_delay = duration - elapsed_seconds - self.cfg.mute.safety_margin_seconds
            
            # Ensure we don't set a negative timer if the ad is already technically over
            actual_delay = max(0.2, calculated_delay)

            self.log.info(
                "Timeline Math: duration=%.1fs | elapsed=%.1fs | delay=%.1fs",
                duration, elapsed_seconds, actual_delay,
                extra=_extra(PROC, trace_id)
            )

            self._unmute_timer = threading.Timer(actual_delay, self._on_timer_fire,
                                                 args=(ad_id, trace_id))
            self._unmute_timer.daemon = True
            self._unmute_timer.start()

        state_set("mic_active", "1")  # ensure mic stays active during mute

    def on_false_positive(self) -> tuple[int | None, str, float]:
        """Called when the user hits the false positive button."""
        with self._lock:
            if not self._muted:
                return None, "", 0.0

            self._cancel_timer()
            ad_id    = self._current_ad_id
            ad_name  = self._current_ad_name
            duration = time.time() - self._mute_start

            self.log.info(fmt_false_positive(ad_name), extra=_extra(PROC))
            self._do_unmute_command(reason="false positive")
            self._muted           = False
            self._current_ad_id   = None
            self._current_ad_name = ""

        return ad_id, ad_name, duration

    def manual_toggle(self) -> bool:
        """Manual mute/unmute from the UI."""
        with self._lock:
            if self._muted:
                self._cancel_timer()
                self._do_unmute_command(reason="manual toggle")
                self._muted = False
            else:
                self._do_mute_command()
                self._muted = True
            return self._muted

    # ── INTERNAL ───────────────────────────────────────────────

    def _on_timer_fire(self, ad_id: int, trace_id: str) -> None:
        """Called by the unmute timer thread."""
        with self._lock:
            if not self._muted or self._current_ad_id != ad_id:
                return   # state changed before timer fired

            actual = time.time() - self._mute_start
            self.log.info(
                fmt_unmute_sent(actual, self._current_ad_name),
                extra=_extra(PROC, trace_id)
            )
            self._do_unmute_command(reason="timer", trace_id=trace_id)
            self._muted = False

            ad_name_snap = self._current_ad_name
            self._current_ad_id   = None
            self._current_ad_name = ""

        self._log_mute_event(ad_id, ad_name_snap, actual, was_false_positive=False)

    def _cancel_timer(self) -> None:
        if self._unmute_timer and self._unmute_timer.is_alive():
            self._unmute_timer.cancel()
        self._unmute_timer = None

    def _do_mute_command(self, trace_id: str = None) -> None:
        backend = self.cfg.mute.backend
        try:
            if backend == "ir":
                cmd = [
                    "irsend", "SEND_ONCE",
                    self.cfg.mute.ir_remote_name,
                    self.cfg.mute.ir_key_mute,
                ]
                detail = f"remote={self.cfg.mute.ir_remote_name}  key={self.cfg.mute.ir_key_mute}"
            else:
                cmd = ["cec-client", "-s", "-d", "1"]
                detail = f"CEC port={self.cfg.mute.cec_port}"

            subprocess.run(cmd, check=True, timeout=2, capture_output=True)
            self.log.info(
                fmt_mute_sent(backend, detail, self._current_ad_name, 0),
                extra=_extra(PROC, trace_id)
            )
        except subprocess.CalledProcessError as exc:
            self.log.error("Mute command failed (exit %d): %s",
                           exc.returncode, exc.stderr.decode(errors="replace"),
                           extra=_extra(PROC, trace_id))
        except subprocess.TimeoutExpired:
            self.log.error("Mute command timed out", extra=_extra(PROC, trace_id))
        except FileNotFoundError:
            self.log.error("irsend not found — is LIRC installed and configured?",
                           extra=_extra(PROC, trace_id))

    def _do_unmute_command(self, reason: str = "", trace_id: str = None) -> None:
        self._do_mute_command(trace_id=trace_id)

    def _log_mute_event(self, ad_id: int, ad_name: str,
                        actual_duration: float,
                        was_false_positive: bool) -> None:
        try:
            with get_conn() as conn:
                conn.execute(
                    """INSERT INTO mute_log
                       (ad_id, ad_name_snapshot, unmuted_at,
                        duration_actual, mute_method, was_false_positive)
                       VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?)""",
                    (ad_id, ad_name, round(actual_duration, 2),
                     self.cfg.mute.backend,
                     1 if was_false_positive else 0)
                )
                conn.commit()
        except sqlite3.Error as exc:
            self.log.error("DB error logging mute event: %s", exc, extra=_extra(PROC))

    def get_status(self) -> dict:
        with self._lock:
            return {
                "is_muted":         self._muted,
                "current_ad_id":    self._current_ad_id,
                "current_ad_name":  self._current_ad_name,
                "mute_start":       self._mute_start if self._muted else None,
            }

# ── PROCESS ENTRY POINT ───────────────────────────────────────

_controller: MuteController | None = None


def get_controller() -> MuteController | None:
    return _controller


def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    global _controller

    cfg = load_config()
    log = setup_logging(PROC, log_level, log_dir)

    _controller = MuteController(cfg, log)

    cmd_thread = threading.Thread(
        target=_command_server,
        args=(_controller, log),
        daemon=True,
        name="mute-cmd-server"
    )
    cmd_thread.start()

    # ── ZMQ ──────────────────────────────────────────────────
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(MATCH_PUB)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_MATCH)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_NEAR_MISS)
    sub.setsockopt(zmq.SUBSCRIBE, TOPIC_STRIKE)

    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    log.info("Ready — backend=%s  debounce=%dms  margin=%.1fs",
             cfg.mute.backend, cfg.mute.debounce_ms,
             cfg.mute.safety_margin_seconds,
             extra=_extra(PROC))

    while running:
        try:
            if not sub.poll(timeout=500):
                continue

            parts = sub.recv_multipart()
            if len(parts) != 2:
                continue

            topic, payload = parts
            data = json.loads(payload)

            if topic == TOPIC_MATCH:
                _controller.on_match(
                    ad_id    = data["ad_id"],
                    ad_name  = data["ad_name"],
                    duration = data["duration"],
                    delta    = data["delta"],
                    score    = data["score"],
                    trace_id = data.get("trace_id", "SYS")
                )

        except zmq.ZMQError as exc:
            if running:
                log.error("ZMQ error: %s", exc, extra=_extra(PROC))
            break
        except Exception as exc:
            log.error("Unexpected: %s", exc, extra=_extra(PROC), exc_info=True)
            time.sleep(0.1)

    sub.close()
    ctx.term()
    log.info("Stopped", extra=_extra(PROC))

def _command_server(controller, log) -> None:
    import zmq
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    rep.bind(MUTE_CTRL)

    log.info("Command server ready on %s", MUTE_CTRL, extra=_extra(PROC))

    while True:
        try:
            if not rep.poll(timeout=500):
                continue
            msg = json.loads(rep.recv_string())
            cmd = msg.get("cmd")

            if cmd == "mute_toggle":
                new_state = controller.manual_toggle()
                rep.send_string(json.dumps({"ok": True, "is_muted": new_state}))

            elif cmd == "false_positive":
                ad_id, ad_name, duration = controller.on_false_positive()
                if ad_id:
                    try:
                        with get_conn() as conn:
                            conn.execute(
                                """INSERT INTO mute_log
                                   (ad_id, ad_name_snapshot, unmuted_at,
                                    duration_actual, mute_method,
                                    was_false_positive)
                                   VALUES (?,?,CURRENT_TIMESTAMP,?,?,1)""",
                                (ad_id, ad_name, round(duration, 2), controller.cfg.mute.backend)
                            )
                            conn.execute("UPDATE ads SET is_active=0 WHERE id=?", (ad_id,))
                            conn.commit()
                    except Exception as e:
                        log.error("DB error on false positive: %s", e, extra=_extra(PROC))
                rep.send_string(json.dumps({"ok": True, "ad_id": ad_id, "ad_name": ad_name}))

            elif cmd == "status":
                rep.send_string(json.dumps({"ok": True, **controller.get_status()}))

            else:
                rep.send_string(json.dumps({"ok": False, "error": f"unknown command: {cmd}"}))

        except Exception as exc:
            log.error("Command server error: %s", exc, extra=_extra(PROC))
            try:
                rep.send_string(json.dumps({"ok": False, "error": str(exc)}))
            except Exception:
                pass

    ctx.destroy(linger=0)

if __name__ == "__main__":
    run()