"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Process 2 — FingerprintWorker

Pulls audio windows from AudioCapture, computes an STFT spectrogram,
extracts constellation peaks, and generates Shazam-style hash pairs.
Forwards results to MatchEngine via ZMQ PUSH.  Runs as two parallel
workers for load balancing across CPU cores.
"""


import os
import sys
import time
import json
import signal
import numpy as np
from scipy import signal as sp_signal
from scipy.ndimage import maximum_filter
import zmq

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config  import load_config, FingerprintConfig
from bus     import AUDIO_PUSH, FINGER_PUSH
from console import setup_logging, _extra, fmt_finger_heartbeat

PROC = "FINGER"


# ── FINGERPRINT PIPELINE ──────────────────────────────────────────────────────

def _normalise_peak(audio: np.ndarray,
                    silence_gate: float = 0.001) -> np.ndarray | None:
    """
    Normalise audio to [-1.0, 1.0] by peak amplitude.
    Returns None if the signal is below the silence gate — no hashes
    will be generated for silent windows.
    """
    peak = np.max(np.abs(audio))
    if peak < silence_gate:
        return None
    return audio / peak


def generate_hashes(audio: np.ndarray,
                    cfg:   FingerprintConfig,
                    sample_rate: int) -> list[tuple[int, int]]:
    """
    Generate Shazam-style constellation hash pairs from a normalised
    audio window.

    Returns a list of (hash_value, time_offset) pairs where:
      hash_value  — 32-bit int encoding (f1, f2, delta_t)
      time_offset — STFT frame index of the anchor peak (t1)
    """
    # Pre-normalise (guard against any residual amplitude drift)
    pre_peak = np.max(np.abs(audio))
    if pre_peak > 0:
        audio = audio / pre_peak

    noverlap = int(cfg.nperseg * cfg.noverlap_ratio)

    _, _, stft = sp_signal.spectrogram(
        audio,
        fs        = sample_rate,
        nperseg   = cfg.nperseg,
        noverlap  = noverlap,
    )

    spec = 10.0 * np.log10(np.abs(stft) + 1e-10)

    threshold = np.percentile(spec, cfg.peak_percentile)
    local_max = maximum_filter(spec, size=cfg.max_filter_size)
    peak_mask = (spec >= threshold) & (spec == local_max)

    freq_idx, time_idx = np.where(peak_mask)
    peaks = list(zip(time_idx.tolist(), freq_idx.tolist()))

    if not peaks:
        return []

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
            f1q = (f1 // 2) * 2
            f2q = (f2 // 2) * 2
            h = (f1q & 0x1FF) | ((f2q & 0x1FF) << 9) | ((delta_t & 0x3FFF) << 18)
            hashes.append((h, t1))

    return hashes


# ── PROCESS ENTRY POINT ───────────────────────────────────────────────────────

def run(worker_id: int = 0,
        log_level: str = "INFO",
        log_dir:   str = "logs") -> None:

    cfg = load_config()
    log = setup_logging(f"{PROC}{worker_id}", log_level, log_dir)

    # ── ZMQ ──────────────────────────────────────────────────────────────────
    ctx      = zmq.Context()
    receiver = ctx.socket(zmq.PULL)
    receiver.connect(AUDIO_PUSH)

    sender = ctx.socket(zmq.PUSH)
    sender.set_hwm(8)
    sender.connect(FINGER_PUSH)

    # ── Stats ─────────────────────────────────────────────────────────────────
    processed      = 0
    total_hashes   = 0
    total_ms       = 0.0
    last_heartbeat = time.time()

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

            # Audio is already bandpass-filtered (300–16 kHz) by AudioCapture.
            # Normalise and generate hashes directly — no second filter applied.
            normalised = _normalise_peak(audio, cfg.audio.silence_threshold)
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
                "hash_count": len(hashes),
                "elapsed_ms": round(elapsed_ms, 1),
                "peak":       meta.get("peak",   0.0),
                "snr_db":     meta.get("snr_db", 0.0),
                "ts":         meta.get("ts",     time.time()),
                "worker_id":  worker_id,
            }).encode()

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