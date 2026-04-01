"""
AdMute v6 — Console Feedback Engine
Rich ANSI terminal output with distributed telemetry tracking.
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
BRIGHT_CYAN   = "\033[96m"
BRIGHT_WHITE  = "\033[97m"

BG_RED    = "\033[41m"
BG_GREEN  = "\033[42m"
BG_YELLOW = "\033[43m"


class AdMuteFormatter(logging.Formatter):
    """
    Custom log formatter that colour-codes by process name and level.
    Injects [trace_id] if present for distributed debugging.
    """

    PROC_COLOURS = {
        "AUDIO":  BRIGHT_BLUE,
        "FINGER": BRIGHT_CYAN,
        "MATCH":  BRIGHT_WHITE,
        "MUTE":   BRIGHT_RED,
        "DAEMON": BRIGHT_MAGENTA if hasattr(__builtins__, 'BRIGHT_MAGENTA') else MAGENTA,
        "API":    YELLOW,
        "DB":     DIM + WHITE,
    }

    def format(self, record: logging.LogRecord) -> str:
        ts    = time.strftime("%H:%M:%S")
        proc  = getattr(record, "proc", "SYS")
        trace = getattr(record, "trace_id", None)
        
        color = self.PROC_COLOURS.get(proc, WHITE)
        tag   = f"{color}[{proc:<6}]{RESET}"
        
        # Telemetry trace injection
        trace_tag = f"{DIM}[{trace}]{RESET} " if trace else ""
        
        msg   = record.getMessage()
        return f"{DIM}{ts}{RESET}  {tag}  {trace_tag}{msg}"


def setup_logging(proc_name: str, log_level: str = "INFO",
                  log_dir: str = "logs") -> logging.Logger:
    """
    Configure and return a logger for a daemon process.
    Outputs to stderr (terminal) with colour + to a rotating file.
    """
    import logging.handlers
    from pathlib import Path

    Path(log_dir).mkdir(exist_ok=True)

    logger = logging.getLogger(f"admute.{proc_name.lower()}")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False

    # ── Terminal handler (coloured) ──────────────────────────
    stream_h = logging.StreamHandler()
    stream_h.setFormatter(AdMuteFormatter())
    logger.addHandler(stream_h)

    # ── File handler (plain text, rotating 5MB × 3) ──────────
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


def _extra(proc: str, trace_id: str = None) -> dict:
    """Helper to inject process name and optional trace ID into log records."""
    d = {"proc": proc}
    if trace_id:
        d["trace_id"] = trace_id
    return d


# ── VISUAL PRIMITIVES ─────────────────────────────────────────

def conf_bar(score: int, threshold: int, width: int = 12) -> str:
    """Render a unicode block progress bar for a confidence score."""
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


def snr_bar(snr_db: float, width: int = 8) -> str:
    """Render an SNR quality bar."""
    ratio  = min(max(snr_db, 0) / 60.0, 1.0)
    filled = int(ratio * width)
    bar    = "▓" * filled + "░" * (width - filled)
    if snr_db >= 50:
        return f"{BRIGHT_GREEN}{bar} {snr_db:.0f}dB{RESET}"
    elif snr_db >= 35:
        return f"{YELLOW}{bar} {snr_db:.0f}dB{RESET}"
    else:
        return f"{RED}{bar} {snr_db:.0f}dB{RESET}"


# ── FEEDBACK FORMATTERS ───────────────────────────────────────

def fmt_audio_heartbeat(peak: float, snr_db: float,
                        chunks_total: int) -> str:
    bar    = snr_bar(snr_db)
    status = f"{DIM}silence{RESET}" if peak < 0.001 else f"peak={peak:.3f}"
    return f"♥  SNR {bar}  {status}  chunks={chunks_total:,}"


def fmt_finger_heartbeat(avg_ms: float, hashes_total: int,
                         workers: int) -> str:
    speed = f"{BRIGHT_GREEN}fast{RESET}" if avg_ms < 60 else \
            f"{YELLOW}ok{RESET}"        if avg_ms < 100 else \
            f"{RED}slow!{RESET}"
    return (f"♥  avg={avg_ms:.0f}ms {speed}  "
            f"hashes={hashes_total:,}  workers={workers}")


def fmt_strike(ad_name: str, score: int, threshold: int) -> str:
    bar = conf_bar(score, threshold)
    pct = int(score / threshold * 100)
    return (f"{YELLOW}· strike ·{RESET}   "
            f"{DIM}\"{ad_name}\"{RESET}  "
            f"conf={score:>4}/{threshold}  ({pct}%)  {bar}")


def fmt_near_miss(ad_name: str, score: int, threshold: int) -> str:
    bar = conf_bar(score, threshold)
    pct = int(score / threshold * 100)
    return (f"{BRIGHT_YELLOW}◉ NEAR MISS{RESET}  "
            f"\"{ad_name}\"  "
            f"conf={score:>4}/{threshold}  ({pct}%)  {bar}")


def fmt_match(ad_name: str, score: int, threshold: int,
              duration: float) -> str:
    bar = conf_bar(score, threshold)
    pct = int(score / threshold * 100)
    return (f"{BOLD}{BG_GREEN} ██ MATCH ██ {RESET}  "
            f"{BRIGHT_GREEN}\"{ad_name}\"{RESET}  "
            f"conf={score:>4}/{threshold}  ({pct}%)  {bar}  "
            f"{BOLD}{duration:.1f}s{RESET}")


def fmt_mute_sent(backend: str, detail: str, ad_name: str,
                  duration: float) -> str:
    return (f"{BOLD}{BG_RED} ► MUTING {RESET}  "
            f"backend={backend}  {detail}  "
            f"ad=\"{ad_name}\"  "
            f"unmute in {duration:.1f}s")


def fmt_unmute_sent(actual_duration: float, ad_name: str) -> str:
    return (f"{BOLD}{BG_GREEN} ◄ UNMUTE {RESET}  "
            f"ad=\"{ad_name}\"  actual={actual_duration:.1f}s")


def fmt_false_positive(ad_name: str) -> str:
    return (f"{BRIGHT_RED}✕ FALSE POSITIVE{RESET}  "
            f"flagged \"{ad_name}\"  unmuting immediately")


def fmt_no_candidates(vault_size: int) -> str:
    if vault_size == 0:
        return (f"{DIM}○  vault is empty — record some ads "
                f"first via the web UI{RESET}")
    return f"{DIM}○  no match  vault={vault_size} ads{RESET}"


def fmt_vault_empty_periodic() -> str:
    return (f"{YELLOW}⚠  Vault is empty.{RESET}  "
            f"Open the web UI at http://<pi-ip>:5001 "
            f"and record your first ad.")