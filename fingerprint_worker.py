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
AdMute v4 — Process 2: FingerprintWorker
Pulls 2-second audio windows from AudioCapture,
applies the full fingerprint pipeline, and pushes
hash lists to the MatchEngine.

Two instances of this process run in parallel (daemon.py
starts them both). Each is an independent process with
its own ZMQ socket — ZMQ PUSH/PULL handles load balancing.
"""

import os
import sys
import time
import json
import signal
import numpy as np
from scipy import signal as sp_signal
from scipy.ndimage import maximum_filter
from scipy.signal import butter, lfilter
import zmq

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config  import load_config, FingerprintConfig
from bus     import AUDIO_PUSH, FINGER_PUSH
from console import setup_logging, _extra, fmt_finger_heartbeat

PROC = "FINGER"


# ── FILTER CACHE ──────────────────────────────────────────────
# Compute once at process start; never inside the hot loop.
_butter_b: np.ndarray | None = None
_butter_a: np.ndarray | None = None


def _init_filter(sample_rate: int) -> None:
    global _butter_b, _butter_a
    nyquist = sample_rate / 2.0
    _butter_b, _butter_a = butter(
        4,
        [400.0 / nyquist, 4500.0 / nyquist],
        btype="band"
    )


def _apply_filter(mono: np.ndarray) -> np.ndarray:
    """Apply pre-computed bandpass filter in-place."""
    return lfilter(_butter_b, _butter_a, mono)


def _normalise_peak(audio: np.ndarray,
                    silence_gate: float = 0.001) -> np.ndarray | None:
    """
    Peak-normalise audio. Returns None if signal is too quiet
    to fingerprint reliably.
    """
    peak = np.max(np.abs(audio))
    if peak < silence_gate:
        return None
    return audio / peak


def generate_hashes(audio: np.ndarray,
                    cfg: FingerprintConfig,
                    sample_rate: int) -> list[tuple[int, int]]:
    """
    Core fingerprinting pipeline.
    Returns list of (hash_value, time_offset) tuples.
    hash_value is a 32-bit int encoding (freq1, freq2, delta_t).
    """
    pre_peak = np.max(np.abs(audio))
    if pre_peak > 0:
        audio = audio / pre_peak

    noverlap = int(cfg.nperseg * cfg.noverlap_ratio)

    _, _, stft = sp_signal.spectrogram(
        audio,
        fs=sample_rate,
        nperseg=cfg.nperseg,
        noverlap=noverlap,
    )

    # Log-power spectrogram
    spec = 10.0 * np.log10(np.abs(stft) + 1e-10)

    # Peak mask: local maxima above the Nth percentile
    threshold  = np.percentile(spec, cfg.peak_percentile)
    local_max  = maximum_filter(spec, size=cfg.max_filter_size)
    peak_mask  = (spec >= threshold) & (spec == local_max)

    # Extract (time_idx, freq_idx) pairs — note spectrogram is freq×time
    freq_idx, time_idx = np.where(peak_mask)
    peaks = list(zip(time_idx.tolist(), freq_idx.tolist()))

    if not peaks:
        return []

    # Combinatorial hashing — anchor + fan target pairs
    hashes: list[tuple[int, int]] = []
    n = len(peaks)

    for i in range(n):
        t1, f1 = peaks[i]
        for j in range(1, cfg.fan_value + 1):
            if i + j >= n:
                break
            t2, f2 = peaks[i + j]
            delta_t = t2 - t1
            if delta_t < 0 or delta_t > cfg.max_time_delta:
                continue
            # Pack into 32-bit int:  f1[9] | f2[9] << 9 | delta_t[14] << 18
            f1q = (f1 // 2) * 2
            f2q = (f2 // 2) * 2
            h = (f1q & 0x1FF) | ((f2q & 0x1FF) << 9) | ((delta_t & 0x3FFF) << 18)
            hashes.append((h, t1))

    return hashes


def run(worker_id: int = 0,
        log_level: str = "INFO",
        log_dir:   str = "logs") -> None:

    cfg = load_config()
    log = setup_logging(f"{PROC}{worker_id}", log_level, log_dir)

    _init_filter(cfg.audio.sample_rate)

    # ── ZMQ ──────────────────────────────────────────────────
    ctx      = zmq.Context()
    receiver = ctx.socket(zmq.PULL)
    receiver.connect(AUDIO_PUSH)

    sender = ctx.socket(zmq.PUSH)
    sender.set_hwm(8)
    sender.connect(FINGER_PUSH)

    # ── Stats ─────────────────────────────────────────────────
    processed      = 0
    total_hashes   = 0
    total_ms       = 0.0
    last_heartbeat = time.time()

    # ── Graceful shutdown ─────────────────────────────────────
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    log.info("Worker %d ready — filter precomputed — waiting for audio",
             worker_id, extra=_extra(PROC))

    while running:
        try:
            # 500ms timeout so we can check the running flag
            if not receiver.poll(timeout=500):
                continue

            parts = receiver.recv_multipart()
            if len(parts) != 2:
                log.warning("Malformed message — skipping", extra=_extra(PROC))
                continue

            meta_bytes, audio_bytes = parts
            meta  = json.loads(meta_bytes)
            audio = np.frombuffer(audio_bytes, dtype=np.float32).copy()

            t_start = time.perf_counter()

            # Pipeline
            filtered    = _apply_filter(audio)
            normalised  = _normalise_peak(filtered, cfg.audio.silence_threshold)

            if normalised is None:
                continue

            hashes = generate_hashes(normalised, cfg.fingerprint,
                                     cfg.audio.sample_rate)

            elapsed_ms = (time.perf_counter() - t_start) * 1000

            processed    += 1
            total_hashes += len(hashes)
            total_ms     += elapsed_ms

            if not hashes:
                log.debug("No hashes generated for this window",
                          extra=_extra(PROC))
                continue

            result_meta = json.dumps({
                "hash_count":  len(hashes),
                "elapsed_ms":  round(elapsed_ms, 1),
                "peak":        meta.get("peak", 0.0),
                "snr_db":      meta.get("snr_db", 0.0),
                "ts":          meta.get("ts", time.time()),
                "worker_id":   worker_id,
            }).encode()

            # Serialise hashes as a flat int32 array: [h0, t0, h1, t1, ...]
            flat = np.array(
                [val for pair in hashes for val in pair],
                dtype=np.int32
            )

            try:
                sender.send_multipart([result_meta, flat.tobytes()],
                                      flags=zmq.NOBLOCK)
            except zmq.Again:
                log.warning("MatchEngine queue full — dropping result",
                            extra=_extra(PROC))

            # Heartbeat every 60s
            now = time.time()
            if now - last_heartbeat >= 60.0:
                avg_ms = total_ms / max(processed, 1)
                log.info(
                    fmt_finger_heartbeat(avg_ms, total_hashes, 1),
                    extra=_extra(PROC)
                )
                last_heartbeat = now

        except zmq.ZMQError as exc:
            if running:
                log.error("ZMQ error: %s", exc, extra=_extra(PROC))
            break
        except Exception as exc:
            log.error("Unexpected error: %s", exc, extra=_extra(PROC),
                      exc_info=True)
            time.sleep(0.1)

    sender.close()
    receiver.close()
    ctx.term()
    log.info("Worker %d stopped", worker_id, extra=_extra(PROC))


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker-id", type=int, default=0)
    args = ap.parse_args()
    run(worker_id=args.worker_id)
