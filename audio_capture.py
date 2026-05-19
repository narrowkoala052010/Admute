"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Process 1 — AudioCapture

Captures audio from the INMP441 I2S microphone, applies digital gain
and a 300 Hz–16 kHz bandpass filter, and feeds a rolling buffer to the
FingerprintWorker pool via ZMQ PUSH.
"""

import os
import sys
import time
import json
import signal
import ctypes
import numpy as np
import zmq
import wave
import threading
from collections import deque

import signal_utils

# ── Silence ALSA/JACK error spam ─────────────────────────────
def _suppress_alsa_errors():
    try:
        asound = ctypes.cdll.LoadLibrary("libasound.so.2")
        asound.snd_lib_error_set_handler(None)
    except Exception:
        pass 

ERROR_HANDLER_FUNC = ctypes.CFUNCTYPE(
    None, ctypes.c_char_p, ctypes.c_int,
    ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p,
)

def _alsa_silent_handler(filename, line, function, err, fmt):
    pass

_c_error_handler = ERROR_HANDLER_FUNC(_alsa_silent_handler)

def _install_silent_handler():
    try:
        asound = ctypes.cdll.LoadLibrary("libasound.so.2")
        asound.snd_lib_error_set_handler(_c_error_handler)
    except Exception:
        pass

_install_silent_handler()

import pyaudio
import io
_devnull = open(os.devnull, 'w')

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config import load_config
from bus    import AUDIO_PUSH, AUDIO_CTRL
from console import setup_logging, _extra, fmt_audio_heartbeat

PROC = "AUDIO"

def _estimate_snr(rms: float, noise_floor: float) -> float:
    if noise_floor < 1e-9:
        return 0.0
    ratio = rms / noise_floor
    return float(20.0 * np.log10(max(ratio, 1e-6)))


def run(log_level: str = "INFO", log_dir: str = "logs") -> None:
    cfg = load_config()
    shared_state = {
        "recording":      False,
        "record_frames":  [],
        "record_start":   0.0,
        "mic_active":     True,
        "last_snr_db":    0.0,
        "last_peak":      0.0,
    }
    log = setup_logging(PROC, log_level, log_dir)
    
    rec_thread = threading.Thread(
        target=_recording_server,
        args=(lambda: shared_state, log),
        daemon=True,
        name="audio-rec-server"
    )
    rec_thread.start()

    # ── ZMQ ──────────────────────────────────────────────────
    ctx    = zmq.Context()
    sender = ctx.socket(zmq.PUSH)
    sender.set_hwm(16)
    sender.bind(AUDIO_PUSH)

    # ── PyAudio ──────────────────────────────────────────────
    _old_stderr = os.dup(2)
    os.dup2(_devnull.fileno(), 2)
    pa = pyaudio.PyAudio()
    os.dup2(_old_stderr, 2)
    os.close(_old_stderr)

    open_kwargs = dict(
        format           = pyaudio.paInt32,
        channels         = cfg.audio.channels,
        rate             = cfg.audio.sample_rate,
        input            = True,
        frames_per_buffer= cfg.audio.chunk_size,
    )
    if cfg.audio.device_index is not None:
        open_kwargs["input_device_index"] = cfg.audio.device_index

    stream = None

    # ── Rolling buffer ────────────────────────────────────────
    chunks_per_window = int(
        cfg.audio.sample_rate / cfg.audio.chunk_size * cfg.audio.buffer_seconds
    )
    half_window = max(1, chunks_per_window // 2)
    buffer: deque = deque(maxlen=chunks_per_window)

    # Build the shared bandpass filter once before the loop
    b, a = signal_utils.build_bandpass(300.0, 16000.0, cfg.audio.sample_rate)

    noise_floor    = 0.001
    noise_alpha    = 0.005
    chunks_total   = 0
    last_heartbeat = time.time()
    last_peak      = 0.0
    last_snr       = 0.0

    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
        log.info("Shutting down", extra=_extra(PROC))
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    log.info("Starting — rate=%d  chunk=%d  window=%.1fs  device=%s",
             cfg.audio.sample_rate, cfg.audio.chunk_size,
             cfg.audio.buffer_seconds,
             cfg.audio.device_index or "default",
             extra=_extra(PROC))

    while running:
        try:
            if stream is None:
                stream = pa.open(**open_kwargs)
                log.info("Audio stream opened", extra=_extra(PROC))

            raw_bytes = stream.read(cfg.audio.chunk_size, exception_on_overflow=False)
            raw = np.frombuffer(raw_bytes, dtype=np.int32)

            # Apply digital gain — boosts mic signal before processing
            mono, peak = signal_utils.apply_digital_gain(raw, gain=30.0)

            rms = float(np.sqrt(np.mean(mono ** 2)))

            if rms < noise_floor or noise_floor < 1e-9:
                noise_floor = noise_floor * (1 - noise_alpha) + rms * noise_alpha
            snr_db = _estimate_snr(rms, noise_floor)

            chunks_total               += 1
            last_peak                   = peak
            last_snr                    = snr_db
            shared_state["last_snr_db"] = snr_db
            shared_state["last_peak"]   = peak

            if not shared_state.get("mic_active", True):
                buffer.clear()
                time.sleep(0.1)
                continue

            now = time.time()
            if now - last_heartbeat >= 60.0:
                log.info(fmt_audio_heartbeat(last_peak, last_snr, chunks_total),
                         extra=_extra(PROC))
                last_heartbeat = now

            if peak < cfg.audio.silence_threshold:
                buffer.clear()
                continue

            # Archive branch: raw amplified frames saved for recording
            if shared_state.get("recording"):
                shared_state["record_frames"].append(mono.copy())

            # Sentinel branch: bandpass-filtered for fingerprint matching
            clean_mono = signal_utils.apply_filter(mono, b, a)
            buffer.append(clean_mono)

            if len(buffer) >= chunks_per_window:
                window = np.concatenate(list(buffer))

                meta = json.dumps({
                    "peak":         peak,
                    "snr_db":       round(snr_db, 1),
                    "chunks_total": chunks_total,
                    "ts":           time.time(),
                }).encode()

                try:
                    sender.send_multipart([meta, window.tobytes()], flags=zmq.NOBLOCK)
                except zmq.Again:
                    log.warning("Worker queue full — dropping audio window",
                                extra=_extra(PROC))

                for _ in range(half_window):
                    if buffer:
                        buffer.popleft()

        except OSError as exc:
            log.error("Audio stream error: %s — reopening in 2s", exc,
                      extra=_extra(PROC))
            if stream:
                try:
                    stream.close()
                except Exception:
                    pass
                stream = None
            time.sleep(2)

        except Exception as exc:
            log.error("Unexpected error: %s", exc, extra=_extra(PROC), exc_info=True)
            time.sleep(1)

    if stream:
        stream.close()
    pa.terminate()
    sender.close()
    ctx.term()
    log.info("Stopped cleanly", extra=_extra(PROC))


def _recording_server(get_state_fn, log) -> None:
    import zmq
    import wave
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    rep.bind(AUDIO_CTRL)

    while True:
        try:
            if not rep.poll(timeout=500):
                continue
            msg = json.loads(rep.recv_string())
            cmd = msg.get("cmd")
            state = get_state_fn()

            if cmd == "start_record":
                state["recording"]     = True
                state["record_frames"] = []
                state["record_start"]  = time.time()
                rep.send_string(json.dumps({"ok": True}))

            elif cmd == "stop_record":
                state["recording"] = False
                frames     = state.get("record_frames", [])
                start_time = state.get("record_start", time.time())
                duration   = round(time.time() - start_time, 2)

                if not frames:
                    rep.send_string(json.dumps({
                        "ok": False, "error": "No audio captured"
                    }))
                    continue

                from pathlib import Path
                from config import load_config
                cfg      = load_config()
                rec_dir  = Path(cfg.paths.recordings_dir)
                rec_dir.mkdir(exist_ok=True)
                filename = f"rec_{int(time.time())}.wav"
                filepath = str(rec_dir / filename)

                audio_data = np.concatenate(frames)
                audio_data = np.nan_to_num(audio_data, nan=0.0, posinf=1.0, neginf=-1.0)
                audio_data = np.clip(audio_data, -1.0, 1.0)
                # Cast to float64 before multiplying — float32 rounds 2_147_483_647
                # to 2_147_483_648 (nearest float32), which overflows int32 on cast.
                # float64 represents all 32-bit integers exactly, no overflow.
                int_data   = (audio_data.astype(np.float64) * 2_147_483_647).astype(np.int32)

                with wave.open(filepath, "wb") as wf:
                    wf.setnchannels(1)
                    wf.setsampwidth(4)
                    wf.setframerate(cfg.audio.sample_rate)
                    wf.writeframes(int_data.tobytes())

                state["record_frames"] = []
                rep.send_string(json.dumps({
                    "ok":       True,
                    "filepath": filepath,
                    "duration": duration,
                }))

            elif cmd == "snr_test":
                # Sample the live SNR over 2.0s — the main loop updates
                # shared_state["last_snr_db"] every ~46ms so this gives
                # ~40 samples and a stable peak reading.
                # 2.0s is acceptable for a user-initiated test; the REP
                # socket is only blocked for this duration on explicit request.
                snr_samples = []
                deadline    = time.time() + 2.0
                while time.time() < deadline:
                    snr_samples.append(state.get("last_snr_db", 0.0))
                    time.sleep(0.05)

                snr = max(snr_samples) if snr_samples else 0.0

                if snr >= 50:
                    classification = "pass"
                    note = "Excellent placement. You're all set."
                elif snr >= 35:
                    classification = "warn"
                    note = "Acceptable. Consider moving mic closer to TV speaker."
                else:
                    classification = "fail"
                    note = "Poor placement. Mutes may be missed. Reposition mic."

                rep.send_string(json.dumps({
                    "ok":             True,
                    "snr_db":         round(snr, 1),
                    "classification": classification,
                    "note":           note,
                }))

            elif cmd == "mic_toggle":
                current             = state.get("mic_active", True)
                state["mic_active"] = not current
                rep.send_string(json.dumps({
                    "ok":        True,
                    "mic_active": state["mic_active"],
                }))

            else:
                rep.send_string(json.dumps({
                    "ok": False, "error": f"unknown command: {cmd}"
                }))

        except Exception as exc:
            log.error("Recording server error: %s", exc, extra=_extra(PROC))
            try:
                rep.send_string(json.dumps({"ok": False, "error": str(exc)}))
            except Exception:
                pass