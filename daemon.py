"""
AdMute v4 — Daemon Orchestrator
Starts all processes, monitors them, restarts any that die,
and handles clean shutdown on SIGTERM / SIGINT.

Run this file to start AdMute:
    python daemon.py
    python daemon.py --log-level DEBUG
"""

import os
import sys
import time
import signal
import argparse
import multiprocessing as mp
from pathlib import Path

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from config  import load_config
from db      import run_migrations, verify_schema
from console import setup_logging, _extra

PROC = "DAEMON"

# Number of fingerprint worker processes.
# 2 keeps the Pi 4B comfortable; drop to 1 if CPU is hot.
FINGERPRINT_WORKERS = 2


# ── PROCESS TARGETS ───────────────────────────────────────────

def _run_audio(log_level, log_dir):
    import audio_capture
    audio_capture.run(log_level=log_level, log_dir=log_dir)


def _run_finger(worker_id, log_level, log_dir):
    import fingerprint_worker
    fingerprint_worker.run(worker_id=worker_id,
                           log_level=log_level, log_dir=log_dir)


def _run_match(log_level, log_dir):
    import match_engine
    match_engine.run(log_level=log_level, log_dir=log_dir)


def _run_mute(log_level, log_dir):
    import mute_controller
    mute_controller.run(log_level=log_level, log_dir=log_dir)

def _run_api(log_level, log_dir):
    import api
    api.run(log_level=log_level, log_dir=log_dir)

# ── DAEMON ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AdMute v4 Daemon")
    parser.add_argument("--log-level", default=None,
                        choices=["DEBUG","INFO","WARNING","ERROR"],
                        help="Override log level from config")
    args = parser.parse_args()

    # Load config first — exits if invalid
    cfg       = load_config()
    log_level = args.log_level or cfg.api.log_level
    log_dir   = cfg.paths.log_dir

    Path(log_dir).mkdir(exist_ok=True)
    Path(cfg.paths.recordings_dir).mkdir(exist_ok=True)

    log = setup_logging(PROC, log_level, log_dir)

    # ── DB ────────────────────────────────────────────────────
    log.info("Running database migrations...", extra=_extra(PROC))
    run_migrations()
    if not verify_schema():
        log.critical("Schema verification failed — aborting", extra=_extra(PROC))
        sys.exit(1)
    log.info("Database ready", extra=_extra(PROC))

    # ── PROCESS DEFINITIONS ───────────────────────────────────
    # Each entry: (name, target_fn, args_tuple)
    process_defs = [
        ("MatchEngine",  _run_match,  (log_level, log_dir)),
        ("MuteCtrl",     _run_mute,   (log_level, log_dir)),
        ("AudioCapture", _run_audio,  (log_level, log_dir)),
	("API",          _run_api,    (log_level, log_dir)),
    ]
    for i in range(FINGERPRINT_WORKERS):
        process_defs.append(
            (f"Finger-{i}", _run_finger, (i, log_level, log_dir))
        )

    # ── START ALL PROCESSES ───────────────────────────────────
    processes: dict[str, mp.Process] = {}

    def start_process(name: str, target, args: tuple) -> mp.Process:
        p = mp.Process(target=target, args=args, name=name, daemon=False)
        p.start()
        log.info("Started  %-14s  pid=%d", name, p.pid, extra=_extra(PROC))
        return p

    for name, target, args in process_defs[:2]:
        processes[name] = start_process(name, target, args)
    time.sleep(0.5)  # give them time to bind

# Start fingerprint workers next — they need to be ready before audio flows
    for name, target, args in process_defs[3:]:  # Finger-0, Finger-1
        processes[name] = start_process(name, target, args)

    time.sleep(2.0)  # wait for workers to fully initialise

# Start AudioCapture last — it will find workers already waiting
    processes["AudioCapture"] = start_process(*process_defs[2])


    # ── BANNER ────────────────────────────────────────────────
    log.info("", extra=_extra(PROC))
    log.info("━" * 52, extra=_extra(PROC))
    log.info("  AdMute v4 is running", extra=_extra(PROC))
    log.info("  Backend : %s", cfg.mute.backend, extra=_extra(PROC))
    log.info("  Threshold: %d  |  Margin: %.1fs",
             cfg.match.confidence_threshold,
             cfg.mute.safety_margin_seconds, extra=_extra(PROC))
    log.info("  Web UI  : http://0.0.0.0:%d", cfg.api.port,
             extra=_extra(PROC))
    log.info("  Logs    : %s/", log_dir, extra=_extra(PROC))
    log.info("━" * 52, extra=_extra(PROC))
    log.info("", extra=_extra(PROC))

    # ── SIGNAL HANDLING ───────────────────────────────────────
    shutdown_requested = False

    def _shutdown(sig, frame):
        nonlocal shutdown_requested
        shutdown_requested = True
        log.info("Shutdown requested (signal %d)", sig, extra=_extra(PROC))

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    # ── MONITOR LOOP ──────────────────────────────────────────
    restart_counts: dict[str, int] = {n: 0 for n, _, _ in process_defs}

    while not shutdown_requested:
        time.sleep(2)

        for name, target, args in process_defs:
            p = processes.get(name)
            if p is None:
                continue

            if not p.is_alive():
                exit_code = p.exitcode
                restart_counts[name] += 1
                count = restart_counts[name]

                log.error(
                    "Process %s died (exit=%s) — restart #%d",
                    name, exit_code, count,
                    extra=_extra(PROC)
                )

                if count >= 5:
                    log.critical(
                        "Process %s has crashed %d times — "
                        "not restarting. Check logs.",
                        name, count, extra=_extra(PROC)
                    )
                    processes[name] = None
                    continue

                # Cool-down before restart to avoid tight crash loops
                backoff = min(2 ** count, 30)
                log.info("Waiting %ds before restarting %s", backoff, name)
                for _ in range(backoff * 10):
                    if shutdown_requested:
                        return
                    time.sleep(0.1)

                processes[name] = start_process(name, target, args)

    # ── CLEAN SHUTDOWN ────────────────────────────────────────
    log.info("Stopping all processes...", extra=_extra(PROC))

    for name, p in reversed(list(processes.items())):
        if p and p.is_alive():
            log.info("Terminating %s (pid=%d)", name, p.pid,
                     extra=_extra(PROC))
            p.terminate()

    # Give processes 5 seconds to exit cleanly
    deadline = time.time() + 5
    for name, p in processes.items():
        if p and p.is_alive():
            remaining = max(0, deadline - time.time())
            p.join(timeout=remaining)
            if p.is_alive():
                log.warning("Force-killing %s", name, extra=_extra(PROC))
                p.kill()

    log.info("AdMute daemon stopped.", extra=_extra(PROC))


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
