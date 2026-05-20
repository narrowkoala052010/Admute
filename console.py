"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Logging setup and structured log format helpers used by all processes.
"""

import time
import logging
from enum import Enum

# ── ANSI CODES ────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

BLACK   = "\033[30m"
RED     = "\033[31m"
GREEN   = "\033[32m"
YELLOW  = "\033[33m"
BLUE    = "\033[34m"
MAGENTA = "\033[35m"
CYAN    = "\033[36m"
WHITE   = "\033[37m"

BRIGHT_RED    = "\033[91m"
BRIGHT_GREEN  = "\033[92m"
BRIGHT_YELLOW = "\033[93m"
BRIGHT_BLUE   = "\033[94m"
BRIGHT_MAGENTA = "\033[95m"
BRIGHT_CYAN   = "\033[96m"
BRIGHT_WHITE  = "\033[97m"

BG_RED    = "\033[41m"
BG_GREEN  = "\033[42m"
BG_YELLOW = "\033[43m"


class AdMuteFormatter(logging.Formatter):
    PROC_COLOURS = {
        "AUDIO":  BRIGHT_BLUE,
        "FINGER": BRIGHT_CYAN,
        "MATCH":  BRIGHT_WHITE,
        "MUTE":   BRIGHT_RED,
        "DAEMON": BRIGHT_MAGENTA,
        "API":    YELLOW,
        "DB":     DIM + WHITE,
    }

    def format(self, record):
        ts    = time.strftime("%H:%M:%S")
        proc  = getattr(record, "proc", "SYS")
        trace = getattr(record, "trace_id", None)
        color = self.PROC_COLOURS.get(proc, WHITE)
        tag   = f"{color}[{proc:<6}]{RESET}"
        trace_tag = f"{DIM}[{trace}]{RESET} " if trace else ""
        msg   = record.getMessage()
        return f"{DIM}{ts}{RESET}  {tag}  {trace_tag}{msg}"


def setup_logging(proc_name, log_level="INFO", log_dir="logs"):
    import logging.handlers
    from pathlib import Path
    Path(log_dir).mkdir(exist_ok=True)
    logger = logging.getLogger(f"admute.{proc_name.lower()}")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False
    stream_h = logging.StreamHandler()
    stream_h.setFormatter(AdMuteFormatter())
    logger.addHandler(stream_h)
    file_h = logging.handlers.RotatingFileHandler(
        Path(log_dir) / f"admute_{proc_name.lower()}.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )
    file_h.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(file_h)
    return logger


def _extra(proc, trace_id=None):
    d = {"proc": proc}
    if trace_id:
        d["trace_id"] = trace_id
    return d


def conf_bar(score, threshold, width=12):
    ratio  = min(score / max(threshold, 1), 1.5)
    filled = int(ratio * width)
    bar    = "█" * min(filled, width) + "░" * max(0, width - filled)
    if ratio >= 1.0:
        return f"{BRIGHT_GREEN}{bar}{RESET}"
    elif ratio >= 0.75:
        return f"{BRIGHT_YELLOW}{bar}{RESET}"
    elif ratio >= 0.35:
        return f"{YELLOW}{bar}{RESET}"
    else:
        return f"{DIM}{bar}{RESET}"


def snr_bar(snr_db, width=8):
    ratio  = min(max(snr_db, 0) / 60.0, 1.0)
    filled = int(ratio * width)
    bar    = "▓" * filled + "░" * (width - filled)
    if snr_db >= 50:
        return f"{BRIGHT_GREEN}{bar} {snr_db:.0f}dB{RESET}"
    elif snr_db >= 35:
        return f"{YELLOW}{bar} {snr_db:.0f}dB{RESET}"
    else:
        return f"{RED}{bar} {snr_db:.0f}dB{RESET}"


def fmt_audio_heartbeat(peak, snr_db, chunks_total):
    bar = snr_bar(snr_db)
    if peak < 0.001:
        status = f"{DIM}silence{RESET}"
    elif peak >= 0.95:
        # Signal is clipping or near-clipping — hashes will be unreliable
        status = f"{BRIGHT_RED}{BOLD}peak={peak:.3f} ⚠ CLIPPING{RESET}"
    else:
        status = f"peak={peak:.3f}"
    return f"♥  SNR {bar}  {status}  chunks={chunks_total:,}"


def fmt_finger_heartbeat(avg_ms, hashes_total, workers):
    speed = f"{BRIGHT_GREEN}fast{RESET}" if avg_ms < 60 else \
            f"{YELLOW}ok{RESET}"        if avg_ms < 100 else \
            f"{RED}slow!{RESET}"
    return (f"♥  avg={avg_ms:.0f}ms {speed}  "
            f"hashes={hashes_total:,}  workers={workers}")


def fmt_strike(ad_name, score, threshold):
    bar = conf_bar(score, threshold)
    pct = int(score / threshold * 100)
    return (f"{YELLOW}· strike ·{RESET}   "
            f"{DIM}\"{ad_name}\"{RESET}  "
            f"conf={score:>4}/{threshold}  ({pct}%)  {bar}")


def fmt_near_miss(ad_name, score, threshold):
    bar = conf_bar(score, threshold)
    pct = int(score / threshold * 100)
    return (f"{BRIGHT_YELLOW}◉ NEAR MISS{RESET}  "
            f"\"{ad_name}\"  "
            f"conf={score:>4}/{threshold}  ({pct}%)  {bar}")


def fmt_match(ad_name, score, threshold, duration):
    bar = conf_bar(score, threshold)
    pct = int(score / threshold * 100)
    return (f"{BOLD}{BG_GREEN} ██ MATCH ██ {RESET}  "
            f"{BRIGHT_GREEN}\"{ad_name}\"{RESET}  "
            f"conf={score:>4}/{threshold}  ({pct}%)  {bar}  "
            f"{BOLD}{duration:.1f}s{RESET}")


def fmt_mute_sent(backend, detail, ad_name, duration):
    return (f"{BOLD}{BG_RED} ► MUTING {RESET}  "
            f"backend={backend}  {detail}  "
            f"ad=\"{ad_name}\"  "
            f"unmute in {duration:.1f}s")


def fmt_unmute_sent(actual_duration, ad_name):
    return (f"{BOLD}{BG_GREEN} ◄ UNMUTE {RESET}  "
            f"ad=\"{ad_name}\"  actual={actual_duration:.1f}s")


def fmt_false_positive(ad_name):
    return (f"{BRIGHT_RED}✕ FALSE POSITIVE{RESET}  "
            f"flagged \"{ad_name}\"  unmuting immediately")


def fmt_no_candidates(vault_size):
    if vault_size == 0:
        return (f"{DIM}○  vault is empty — record some ads "
                f"first via the web UI{RESET}")
    return f"{DIM}○  no match  vault={vault_size} ads{RESET}"


def fmt_vault_empty_periodic():
    return (f"{YELLOW}⚠  Vault is empty.{RESET}  "
            f"Open the web UI at http://<pi-ip>:5001 "
            f"and record your first ad.")