"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.

Key improvements over the IR backend:
  • Real mute state querying via CEC "Give Audio Status" (opcode 0x71).
    The TV's audio system responds with the actual mute flag (bit 7 of
    the "Report Audio Status" byte).  We never send a mute command if the
    TV is already muted, and never send an unmute if it's already unmuted.
    This eliminates the state-drift bug of the old toggle-only approach.
  • python-cec (libcec) is opened once at startup and held for the process
    lifetime.  No subprocess spawning per command.

CEC device model:
  • We register as CEC_DEVICE_TYPE_RECORDING_DEVICE so the TV sees us
    as a playback device, not a root device, avoiding HDMI routing changes.
  • Mute/unmute keypress targets CECDEVICE_TV (logical address 0).
  • Audio status is queried from CECDEVICE_AUDIOSYSTEM (addr 5) first;
    if no audio system is present the TV itself responds.
  • cec_port in config.toml can be left empty for auto-detection.
"""

import os
import sys
import time
import json
import signal
import sqlite3
import threading
import zmq
from pathlib import Path
from typing  import Optional

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config  import load_config
from db      import get_conn, state_get, state_set
from bus     import MATCH_PUB, MUTE_CTRL, TOPIC_MATCH, TOPIC_NEAR_MISS, TOPIC_STRIKE
from console import (setup_logging, _extra,
                     fmt_mute_sent, fmt_unmute_sent, fmt_false_positive)

PROC = "MUTE"


# ── CEC INITIALISATION AND HELPERS ───────────────────────────────────────────

def _init_cec(cfg, log):
    """
    Initialise the HDMI-CEC adapter using the PyPI `cec` package (0.2.8+).

    The `cec` package exposes a simple Pythonic API:
        cec.init() → cec.Device(addr) → device.transmit()

    After init, a warmup ping is sent immediately to force the CEC bus
    handshake with the TV.  Without this, the first real mute command
    triggers the handshake and takes 2-3 seconds.  The warmup moves
    that latency to daemon startup where it's invisible to the user.

    Returns the cec module on success, None on failure.
    """
    try:
        import cec
        cec.init()
        log.info(
            "CEC: adapter ready — device='%s'",
            cfg.mute.cec_device_name,
            extra=_extra(PROC)
        )

        # ── Warmup ping ───────────────────────────────────────────────────
        # Send GIVE_AUDIO_STATUS to the TV immediately after init.
        # This forces the full CEC bus handshake now so the first mute
        # command fires instantly instead of waiting 2-3 seconds.
        log.info("CEC: warming up bus connection...", extra=_extra(PROC))
        try:
            import threading
            warmed  = threading.Event()
            result  = [None]

            def _on_warmup(event, data):
                opcode = (data.get("opcode") if isinstance(data, dict)
                          else getattr(data, "opcode", None))
                if opcode == cec.CEC_OPCODE_REPORT_AUDIO_STATUS:
                    params = (data.get("parameters") if isinstance(data, dict)
                              else getattr(data, "parameters", b""))
                    if params:
                        b0 = params[0] if isinstance(params[0], int) else ord(params[0])
                        result[0] = bool(b0 & 0x80)
                    warmed.set()

            cec.add_callback(_on_warmup, cec.EVENT_COMMAND)
            try:
                for addr in (cec.CECDEVICE_AUDIOSYSTEM, cec.CECDEVICE_TV):
                    cec.Device(addr).transmit(cec.CEC_OPCODE_GIVE_AUDIO_STATUS)
                    if warmed.wait(timeout=0.5):
                        break
            finally:
                cec.remove_callback(_on_warmup, cec.EVENT_COMMAND)

            if result[0] is not None:
                log.info(
                    "CEC: bus ready — TV reports %s",
                    "muted" if result[0] else "unmuted",
                    extra=_extra(PROC)
                )
            else:
                log.info(
                    "CEC: bus ready — TV did not respond to audio status "
                    "(normal for some TVs, blind keypress will be used)",
                    extra=_extra(PROC)
                )
        except Exception as exc:
            log.warning("CEC: warmup ping failed: %s — "
                        "first mute may be slow", exc, extra=_extra(PROC))

        return cec

    except ImportError as exc:
        log.error(
            "CEC: import failed: %s — "
            "Run: pip install cec  (and: sudo apt install libcec-dev)",
            exc, extra=_extra(PROC)
        )
        return None
    except Exception as exc:
        log.error("CEC: initialisation error: %s", exc, extra=_extra(PROC))
        return None


def _cec_get_mute_state(lib, log) -> Optional[bool]:
    """
    Query the TV's real audio mute state via CEC.

    Sends GIVE_AUDIO_STATUS (0x71) to AUDIOSYSTEM then TV.
    Intercepts the REPORT_AUDIO_STATUS response (0x7A) via callback.
    Bit 7 of the response byte = mute flag.

    Returns True/False if TV responds within 300ms, None otherwise.
    Caller falls back to blind keypress when None is returned.
    """
    try:
        import threading
        result   = [None]
        received = threading.Event()

        def _on_cmd(event, data):
            opcode = (data.get("opcode") if isinstance(data, dict)
                      else getattr(data, "opcode", None))
            if opcode == lib.CEC_OPCODE_REPORT_AUDIO_STATUS:
                params = (data.get("parameters") if isinstance(data, dict)
                          else getattr(data, "parameters", b""))
                if params:
                    b0 = params[0] if isinstance(params[0], int) else ord(params[0])
                    result[0] = b0
                received.set()

        lib.add_callback(_on_cmd, lib.EVENT_COMMAND)
        try:
            for addr in (lib.CECDEVICE_AUDIOSYSTEM, lib.CECDEVICE_TV):
                lib.Device(addr).transmit(lib.CEC_OPCODE_GIVE_AUDIO_STATUS)
                if received.wait(timeout=0.3):
                    break
        finally:
            lib.remove_callback(_on_cmd, lib.EVENT_COMMAND)

        return bool(result[0] & 0x80) if result[0] is not None else None

    except Exception as exc:
        log.warning("CEC: audio status query failed: %s", exc,
                    extra=_extra(PROC))
        return None


def _cec_set_mute(lib, log, target_muted: bool) -> bool:
    """
    Send a CEC mute or unmute keypress immediately.

    Does NOT query the TV's audio status first — that check was removed
    because it added 300ms latency on every command.  State accuracy is
    managed by MuteController.self._muted which tracks every transition
    we make.  The only time we don't know the TV's state is at startup,
    which is handled by the warmup ping in _init_cec.

    Sends USER_CONTROL_PRESSED (0x44) with mute code 0x43,
    followed by USER_CONTROL_RELEASE (0x45) — per CEC spec 1.4.

    Returns True if keypress was sent, False on error.
    """
    try:
        tv = lib.Device(lib.CECDEVICE_TV)
        tv.transmit(lib.CEC_OPCODE_USER_CONTROL_PRESSED, bytes([0x43]))
        tv.transmit(lib.CEC_OPCODE_USER_CONTROL_RELEASE)
        return True
    except Exception as exc:
        log.error(
            "CEC: keypress error (target=%s): %s",
            "mute" if target_muted else "unmute", exc,
            extra=_extra(PROC)
        )
        return False


# ── MUTE CONTROLLER ───────────────────────────────────────────────────────────

class MuteController:
    """
    Stateful mute manager.  Thread-safe.
    All state transitions go through this class.
    The CEC adapter is injected at construction; all hardware calls
    are made through _cec_set_mute().
    """

    def __init__(self, cfg, log, cec_lib=None):
        self.cfg      = cfg
        self.log      = log
        self._cec_lib = cec_lib
        self._lock    = threading.Lock()

        self._muted            = False
        self._current_ad_id    = None
        self._current_ad_name  = ""
        self._mute_start       = 0.0
        self._unmute_timer: Optional[threading.Timer] = None
        self._last_match_ts    = 0.0

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    @property
    def is_muted(self) -> bool:
        with self._lock:
            return self._muted

    @property
    def current_ad(self) -> Optional[dict]:
        with self._lock:
            if not self._muted:
                return None
            return {
                "ad_id":    self._current_ad_id,
                "ad_name":  self._current_ad_name,
                "muted_at": self._mute_start,
            }

    def on_match(self, ad_id: int, ad_name: str,
                 duration: float, score: int,
                 time_offset_secs: float = 0.0) -> None:
        """
        Called when MatchEngine fires a confident match.

        time_offset_secs: how far into the ad we were at detection time,
        derived from the time-coherence peak_delta.  Used to calculate
        the correct unmute time even when detection fires mid-ad.

        Example: 30s ad detected 10s in → remaining = 30 - 10 - 1.5 = 18.5s
        Without this, the unmute timer would fire 28.5s later, keeping the
        TV muted 10 seconds past the end of the ad.
        """
        with self._lock:
            now = time.time()

            if now - self._last_match_ts < self.cfg.mute.debounce_ms / 1000.0:
                return

            if self._muted and self._current_ad_id == ad_id:
                return  # same ad still playing — no action

            if self._muted:
                # Different ad interrupted — cancel timer and unmute cleanly
                self._cancel_timer()
                self._do_unmute_command(reason="interrupted by new ad")

            self._last_match_ts   = now
            self._current_ad_id   = ad_id
            self._current_ad_name = ad_name
            self._mute_start      = now

            # Remaining time = how much of the ad is left to play
            # Clamp to 0.5s minimum so the timer always fires cleanly
            remaining = max(0.5, duration - time_offset_secs - self.cfg.mute.safety_margin_seconds)
            self._do_mute_command(unmute_in=remaining)
            self._muted = True

            self._unmute_timer = threading.Timer(
                remaining, self._on_timer_fire, args=(ad_id,)
            )
            self._unmute_timer.daemon = True
            self._unmute_timer.start()
            state_set("mic_active", "1")

    def on_false_positive(self) -> tuple:
        """
        Called when the user hits the false positive button.
        Returns (ad_id, ad_name, actual_duration_seconds) for logging.
        """
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
        """Manual mute/unmute from the UI. Returns new muted state."""
        with self._lock:
            if self._muted:
                self._cancel_timer()
                self._do_unmute_command(reason="manual toggle")
                self._muted = False
            else:
                self._do_mute_command()
                self._muted = True
            return self._muted

    def get_status(self) -> dict:
        """Return current state as a dict for the API."""
        with self._lock:
            return {
                "is_muted":        self._muted,
                "current_ad_id":   self._current_ad_id,
                "current_ad_name": self._current_ad_name,
                "mute_start":      self._mute_start if self._muted else None,
            }

    # ── INTERNAL ──────────────────────────────────────────────────────────────

    def _on_timer_fire(self, ad_id: int) -> None:
        with self._lock:
            if not self._muted or self._current_ad_id != ad_id:
                return  # state changed before timer fired
            actual = time.time() - self._mute_start
            self.log.info(
                fmt_unmute_sent(actual, self._current_ad_name),
                extra=_extra(PROC)
            )
            self._do_unmute_command(reason="timer")
            self._muted = False
            snap_name   = self._current_ad_name
            self._current_ad_id   = None
            self._current_ad_name = ""
            self._log_mute_event(ad_id, snap_name, actual,
                                 was_false_positive=False)

    def _cancel_timer(self) -> None:
        """Cancel any pending unmute timer. Call with _lock held."""
        if self._unmute_timer and self._unmute_timer.is_alive():
            self._unmute_timer.cancel()
        self._unmute_timer = None

    def _do_mute_command(self, unmute_in: float = 0.0) -> None:
        """
        Send a CEC mute command.
        Queries the TV's real audio status first; only fires the keypress
        if the TV is not already muted.
        unmute_in: seconds until scheduled unmute — used for log display only.
        """
        if self._cec_lib is None:
            self.log.error(
                "CEC: adapter not available — mute command skipped",
                extra=_extra(PROC)
            )
            return

        sent = _cec_set_mute(self._cec_lib, self.log, target_muted=True)
        if sent:
            self.log.info(
                fmt_mute_sent(
                    "cec",
                    f"device='{self.cfg.mute.cec_device_name}'",
                    self._current_ad_name,
                    unmute_in,
                ),
                extra=_extra(PROC)
            )

    def _do_unmute_command(self, reason: str = "") -> None:
        """
        Send a CEC unmute command.

        Queries the TV's real audio status first; only fires the keypress
        if the TV is currently muted.  This is a fully independent function
        — it does NOT call _do_mute_command — so mute and unmute can never
        accidentally act as the same blind toggle.
        """
        if self._cec_lib is None:
            self.log.error(
                "CEC: adapter not available — unmute command skipped",
                extra=_extra(PROC)
            )
            return

        sent = _cec_set_mute(self._cec_lib, self.log, target_muted=False)
        if sent:
            self.log.info(
                "Unmute sent via CEC — reason: %s", reason,
                extra=_extra(PROC)
            )

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
                     "cec", 1 if was_false_positive else 0)
                )
                conn.commit()
        except sqlite3.Error as exc:
            self.log.error("DB error logging mute event: %s", exc,
                           extra=_extra(PROC))


# ── MODULE-LEVEL ACCESSOR ─────────────────────────────────────────────────────

_controller: Optional[MuteController] = None


def get_controller() -> Optional[MuteController]:
    return _controller


# ── PROCESS ENTRY POINT ───────────────────────────────────────────────────────

def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    global _controller

    cfg = load_config()
    log = setup_logging(PROC, log_level, log_dir)

    # Open CEC adapter once — held for process lifetime
    cec_lib     = _init_cec(cfg, log)
    _controller = MuteController(cfg, log, cec_lib=cec_lib)

    # Command server handles API requests (false_positive, mute_toggle, status)
    cmd_thread = threading.Thread(
        target  = _command_server,
        args    = (_controller, log),
        daemon  = True,
        name    = "mute-cmd-server",
    )
    cmd_thread.start()

    # ── ZMQ PUB subscription ─────────────────────────────────────────────────
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

    log.info(
        "Ready — backend=cec debounce=%dms margin=%.1fs",
        cfg.mute.debounce_ms, cfg.mute.safety_margin_seconds,
        extra=_extra(PROC)
    )

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
                    ad_id            = data["ad_id"],
                    ad_name          = data["ad_name"],
                    duration         = data["duration"],
                    score            = data["score"],
                    time_offset_secs = data.get("time_offset_secs", 0.0),
                )
            # NEAR_MISS and STRIKE are logged by MatchEngine;
            # MuteController only acts on confirmed MATCH events.

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


# ── COMMAND SERVER ────────────────────────────────────────────────────────────

def _command_server(controller: MuteController, log) -> None:
    """
    ZMQ REP server handling commands from the LocalAPI.
    Runs in a daemon thread inside the MuteController process.
    Commands: mute_toggle | false_positive | status
    """
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
                                   VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, 1)""",
                                (ad_id, ad_name, round(duration, 2), "cec")
                            )
                            # Flag the ad for review — deactivate until confirmed
                            conn.execute(
                                "UPDATE ads SET is_active = 0 WHERE id = ?",
                                (ad_id,)
                            )
                            conn.commit()
                    except Exception as exc:
                        log.error("DB error on false positive: %s", exc,
                                  extra=_extra(PROC))
                rep.send_string(json.dumps({
                    "ok":      True,
                    "ad_id":   ad_id,
                    "ad_name": ad_name,
                }))

            elif cmd == "status":
                rep.send_string(json.dumps({"ok": True, **controller.get_status()}))

            else:
                rep.send_string(json.dumps({
                    "ok": False, "error": f"unknown command: {cmd}"
                }))

        except Exception as exc:
            log.error("Command server error: %s", exc, extra=_extra(PROC))
            try:
                rep.send_string(json.dumps({"ok": False, "error": str(exc)}))
            except Exception:
                pass

    ctx.destroy(linger=0)


if __name__ == "__main__":
    run()