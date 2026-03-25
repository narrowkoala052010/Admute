"""
AdMute v4 — ZeroMQ Bus Addresses
Single source of truth for all inter-process socket addresses.
"""

# P1 AudioCapture  →  P2 FingerprintWorker
# Pattern: PUSH / PULL  (load-balanced across worker count)
AUDIO_PUSH = "tcp://127.0.0.1:5570"

# P2 FingerprintWorker  →  P3 MatchEngine
# Pattern: PUSH / PULL
FINGER_PUSH = "tcp://127.0.0.1:5571"

# P3 MatchEngine  →  P4 MuteController
# Pattern: PUB / SUB  (broadcast; future-proof for multiple consumers)
MATCH_PUB = "tcp://127.0.0.1:5572"

# P5 LocalAPI  →  P4 MuteController  (commands: mute, unmute, false-positive)
MUTE_CTRL = "tcp://127.0.0.1:5573"

# P5 LocalAPI  →  P1 AudioCapture  (commands: start_record, stop_record, snr_test)
AUDIO_CTRL = "tcp://127.0.0.1:5574"

# ── TOPIC CONSTANTS ───────────────────────────────────────────
# Published on MATCH_PUB, consumed by MuteController
TOPIC_MATCH     = b"match"       # confident match — trigger mute
TOPIC_NEAR_MISS = b"near_miss"   # high confidence but below threshold
TOPIC_STRIKE    = b"strike"      # low-mid confidence partial match
TOPIC_SILENCE   = b"silence"     # chunk was below silence threshold
TOPIC_HEARTBEAT = b"heartbeat"   # periodic liveness signal
