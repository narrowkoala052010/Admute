"""
AdMute v4 — Config Loader
Loads and validates config.toml into typed dataclasses.
Call load_config() once at startup; pass the Config object around.
"""

import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

CONFIG_PATH = Path(__file__).parent / "config.toml"


@dataclass
class AudioConfig:
    sample_rate:       int
    chunk_size:        int
    channels:          int
    device_index:      Optional[int]
    silence_threshold: float
    buffer_seconds:    float


@dataclass
class FingerprintConfig:
    nperseg:         int
    noverlap_ratio:  float
    peak_percentile: float
    max_filter_size: int
    fan_value:       int
    max_time_delta:  int


@dataclass
class MatchConfig:
    confidence_threshold: int
    cooldown_seconds:     float
    strike_min:           int
    near_miss_ratio:      float
    auto_link_threshold:  float


@dataclass
class MuteConfig:
    backend:               str
    safety_margin_seconds: float
    debounce_ms:           int
    ir_remote_name:        str
    ir_key_mute:           str
    cec_port:              int
    sonar_ping_ms:         int


@dataclass
class ApiConfig:
    port:      int
    log_level: str


@dataclass
class PathsConfig:
    db:             str
    recordings_dir: str
    log_dir:        str


@dataclass
class Config:
    audio:       AudioConfig
    fingerprint: FingerprintConfig
    match:       MatchConfig
    mute:        MuteConfig
    api:         ApiConfig
    paths:       PathsConfig


def load_config(path: Path = CONFIG_PATH) -> Config:
    if not path.exists():
        print(f"[FATAL] config.toml not found at {path}", file=sys.stderr)
        sys.exit(1)

    with open(path, "rb") as f:
        raw = tomllib.load(f)

    try:
        a  = raw["audio"]
        fp = raw["fingerprint"]
        m  = raw["match"]
        mu = raw["mute"]
        ap = raw["api"]
        p  = raw["paths"]

        cfg = Config(
            audio=AudioConfig(
                sample_rate       = int(a["sample_rate"]),
                chunk_size        = int(a["chunk_size"]),
                channels          = int(a["channels"]),
                device_index      = a.get("device_index"),
                silence_threshold = float(a["silence_threshold"]),
                buffer_seconds    = float(a["buffer_seconds"]),
            ),
            fingerprint=FingerprintConfig(
                nperseg         = int(fp["nperseg"]),
                noverlap_ratio  = float(fp["noverlap_ratio"]),
                peak_percentile = float(fp["peak_percentile"]),
                max_filter_size = int(fp["max_filter_size"]),
                fan_value       = int(fp["fan_value"]),
                max_time_delta  = int(fp["max_time_delta"]),
            ),
            match=MatchConfig(
                confidence_threshold = int(m["confidence_threshold"]),
                cooldown_seconds     = float(m["cooldown_seconds"]),
                strike_min           = int(m["strike_min"]),
                near_miss_ratio      = float(m["near_miss_ratio"]),
                auto_link_threshold  = float(m.get("auto_link_threshold", 0.85)), # NEW
            ),
            mute=MuteConfig(
                backend               = str(mu["backend"]),
                safety_margin_seconds = float(mu["safety_margin_seconds"]),
                debounce_ms           = int(mu["debounce_ms"]),
                ir_remote_name        = str(mu["ir_remote_name"]),
                ir_key_mute           = str(mu["ir_key_mute"]),
                cec_port              = int(mu.get("cec_port", 1)),
                sonar_ping_ms         = int(mu.get("sonar_ping_ms", 250)), # NEW
            ),
            api=ApiConfig(
                port      = int(ap["port"]),
                log_level = str(ap["log_level"]),
            ),
            paths=PathsConfig(
                db             = str(p["db"]),
                recordings_dir = str(p["recordings_dir"]),
                log_dir        = str(p["log_dir"]),
            ),
        )

        _validate(cfg)
        return cfg

    except KeyError as e:
        print(f"[FATAL] Missing config key: {e}", file=sys.stderr)
        sys.exit(1)
    except (ValueError, TypeError) as e:
        print(f"[FATAL] Invalid config value: {e}", file=sys.stderr)
        sys.exit(1)


def _validate(cfg: Config) -> None:
    errors = []

    if cfg.audio.sample_rate not in (44100, 48000):
        errors.append(f"audio.sample_rate must be 44100 or 48000, got {cfg.audio.sample_rate}")
    if cfg.audio.chunk_size not in (1024, 2048, 4096, 8192):
        errors.append(f"audio.chunk_size must be a power of 2 (1024–8192)")
    if cfg.mute.backend not in ("ir", "cec"):
        errors.append(f"mute.backend must be 'ir' or 'cec', got '{cfg.mute.backend}'")
    if cfg.match.confidence_threshold < 10:
        errors.append(f"match.confidence_threshold seems dangerously low: {cfg.match.confidence_threshold}")
    if not (0.0 < cfg.match.near_miss_ratio < 1.0):
        errors.append(f"match.near_miss_ratio must be between 0.0 and 1.0")

    if errors:
        for e in errors:
            print(f"[FATAL] Config error: {e}", file=sys.stderr)
        sys.exit(1)
