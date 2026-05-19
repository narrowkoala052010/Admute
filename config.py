"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Configuration loader — reads config.toml and exposes typed dataclasses
for every section.  All processes call load_config() at startup.
"""

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import tomllib                          # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib             # pip install tomli  (3.8–3.10)
    except ImportError:
        import tomllib                      # will raise a clean error if neither present

BASE    = Path(__file__).parent.resolve()
TOML    = BASE / "config.toml"


# ── SECTION DATACLASSES ───────────────────────────────────────────────────────

@dataclass
class AudioConfig:
    sample_rate:       int
    chunk_size:        int
    buffer_seconds:    float
    channels:          int
    silence_threshold: float
    device_index:      Optional[int]


@dataclass
class FingerprintConfig:
    nperseg:         int
    noverlap_ratio:  float
    peak_percentile: int
    max_filter_size: int
    fan_value:       int
    max_time_delta:  int


@dataclass
class MatchConfig:
    confidence_threshold: int
    near_miss_ratio:      float
    strike_min:           int
    cooldown_seconds:     float


@dataclass
class CacheConfig:
    """
    Tiered RAM cache parameters for the MatchEngine.

    L1 (Kings):   top l1_size ads by 30-day mute_log count — resident dict
    L2 (Context): last l2_size heard ads — OrderedDict, tail = most recent
    L3 (Vault):   SQLite fallback for everything else

    The MatchEngine runs a heartbeat every heartbeat_seconds to sync the
    cache against the DB (new ads, deactivations, L1 recalculation).
    """
    l1_size:           int    = 5
    l2_size:           int    = 50
    l1_window_days:    int    = 30
    heartbeat_seconds: int    = 60


@dataclass
class MuteConfig:
    backend:               str            # "cec" | "ir"
    debounce_ms:           int
    safety_margin_seconds: float

    # CEC
    cec_port:              str            # empty = auto-detect
    cec_device_name:       str

    # IR (legacy)
    ir_remote_name:        str
    ir_key_mute:           str


@dataclass
class ApiConfig:
    port:      int
    log_level: str


@dataclass
class PathsConfig:
    recordings_dir: str
    log_dir:        str


@dataclass
class AdMuteConfig:
    audio:       AudioConfig
    fingerprint: FingerprintConfig
    match:       MatchConfig
    cache:       CacheConfig
    mute:        MuteConfig
    api:         ApiConfig
    paths:       PathsConfig


# ── LOADER ────────────────────────────────────────────────────────────────────

def load_config(path: Path = TOML) -> AdMuteConfig:
    with open(path, "rb") as f:
        raw = tomllib.load(f)

    a = raw["audio"]
    fp = raw["fingerprint"]
    m = raw["match"]
    c = raw.get("cache", {})
    mu = raw["mute"]
    ap = raw["api"]
    pa = raw.get("paths", {})

    return AdMuteConfig(
        audio=AudioConfig(
            sample_rate       = int(a["sample_rate"]),
            chunk_size        = int(a["chunk_size"]),
            buffer_seconds    = float(a["buffer_seconds"]),
            channels          = int(a["channels"]),
            silence_threshold = float(a["silence_threshold"]),
            device_index      = a.get("device_index"),
        ),
        fingerprint=FingerprintConfig(
            nperseg         = int(fp["nperseg"]),
            noverlap_ratio  = float(fp["noverlap_ratio"]),
            peak_percentile = int(fp["peak_percentile"]),
            max_filter_size = int(fp["max_filter_size"]),
            fan_value       = int(fp["fan_value"]),
            max_time_delta  = int(fp["max_time_delta"]),
        ),
        match=MatchConfig(
            confidence_threshold = int(m["confidence_threshold"]),
            near_miss_ratio      = float(m["near_miss_ratio"]),
            strike_min           = int(m["strike_min"]),
            cooldown_seconds     = float(m["cooldown_seconds"]),
        ),
        cache=CacheConfig(
            l1_size           = int(c.get("l1_size", 5)),
            l2_size           = int(c.get("l2_size", 50)),
            l1_window_days    = int(c.get("l1_window_days", 30)),
            heartbeat_seconds = int(c.get("heartbeat_seconds", 60)),
        ),
        mute=MuteConfig(
            backend               = str(mu.get("backend", "cec")),
            debounce_ms           = int(mu.get("debounce_ms", 1500)),
            safety_margin_seconds = float(mu.get("safety_margin_seconds", 1.5)),
            cec_port              = str(mu.get("cec_port", "")),
            cec_device_name       = str(mu.get("cec_device_name", "AdMute")),
            ir_remote_name        = str(mu.get("ir_remote_name", "TV")),
            ir_key_mute           = str(mu.get("ir_key_mute", "KEY_MUTE")),
        ),
        api=ApiConfig(
            port      = int(ap["port"]),
            log_level = str(ap.get("log_level", "INFO")),
        ),
        paths=PathsConfig(
            recordings_dir = str(pa.get("recordings_dir", "recordings")),
            log_dir        = str(pa.get("log_dir", "logs")),
        ),
    )