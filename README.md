# AdMute Light 🔇

**An open-source acoustic ad-muter. Runs on a Raspberry Pi.**

Modern streaming apps — Roku, WebOS, Fire TV, Apple TV — have made traditional ad blockers obsolete. You can't install a browser extension on a smart TV. Blocking the DNS often breaks the video player. Server-Side Ad Insertion (SSAI) makes network-level filtering useless.

AdMute Light takes a different approach entirely. It listens to the room.

It runs on a Raspberry Pi sitting next to your TV. It uses a small I2S microphone to capture ambient audio, generates real-time acoustic fingerprints using a Shazam-style algorithm, and fires an **HDMI-CEC mute command** the moment it recognises a commercial. When the ad ends, it unmutes. No network interference. No app modifications. No accounts. Just silence.

> You own the TV, the remote, and the air in your room. AdMute Light automates the mute button.

---

## ✅ What it does

- Detects TV commercials in real time using acoustic fingerprinting
- Mutes via **HDMI-CEC** — no IR blaster required (works over the HDMI cable you already have)
- Learns ads from your own TV: record once, mute forever
- Fully local — no cloud, no telemetry, no accounts
- Web UI accessible from your phone on the same WiFi

## ❌ What it doesn't do

- It doesn't block ads at the network level (that's Pi-hole's job)
- It doesn't work until you've taught it your ads (you record them yourself)
- It doesn't automatically share or receive fingerprints — your vault is yours

---

## 🛡️ Privacy

- **100% local.** Everything runs on your Pi. Nothing leaves your network.
- **No audio storage.** The microphone captures audio, generates mathematical hashes, and discards the raw audio. Recorded WAV files are only kept until you ingest them into the vault, then deleted.
- **Analog air-gap.** Streaming apps are completely blind to what AdMute is doing. There is nothing to detect and no anti-automation scripts to trigger.
- **Cryptographic API.** The web UI communicates with the Pi via HMAC-SHA256 signed requests with 60-second replay-attack protection.

---

## 🏗️ Architecture

AdMute Light runs as five isolated processes communicating over ZeroMQ at RAM speed — no shared memory, no GIL contention, no dropped audio frames.

```
INMP441 mic
    │
    ▼
[P1] AudioCapture          — reads I2S audio, applies 300–16kHz bandpass, digital gain
    │  ZMQ PUSH
    ▼
[P2] FingerprintWorker ×2  — parallel STFT spectrogram + constellation hash generation
    │  ZMQ PUSH
    ▼
[P3] MatchEngine           — L1/L2/L3 tiered RAM cache + SQLite vault matching
    │  ZMQ PUB (match events)
    ▼
[P4] MuteController        — HDMI-CEC mute/unmute with real audio status query
    │
[P5] API (aiohttp)         — REST API + web UI served on port 5001
```

**Tiered cache (L1/L2/L3):**
- **L1 (Kings):** Top 5 most-detected ads held in resident RAM — sub-millisecond lookup
- **L2 (Context):** Last 50 detected ads in an ordered RAM cache — promotes on each hit
- **L3 (Vault):** SQLite fallback for everything else — L3 hits are automatically warmed into L2

---

## 🔧 Hardware

| Part | Notes |
|---|---|
| Raspberry Pi 4B (2GB+) | Any Pi 4 works. Pi 5 also works. |
| INMP441 I2S microphone | ~$3. Wired to GPIO pins. |
| HDMI cable | Already connected to your TV. CEC runs over it. |
| MicroSD card (16GB+) | Class 10 or better. |

No IR blaster required. AdMute Light communicates with your TV over the HDMI cable you already have via the HDMI-CEC protocol (marketed as Anynet+ on Samsung, SimpLink on LG, BRAVIA Sync on Sony). Enable it in your TV's settings if it isn't already.

**INMP441 wiring (I2S):**

| INMP441 Pin | Raspberry Pi GPIO |
|---|---|
| VDD | 3.3V (Pin 1) |
| GND | Ground (Pin 6) |
| SD | GPIO 20 (Pin 38) |
| WS | GPIO 19 (Pin 35) |
| SCK | GPIO 18 (Pin 12) |
| L/R | Ground (left channel) |

---

## 📦 Installation

### 1. System dependencies

```bash
sudo apt update
sudo apt install libcec-dev cec-utils portaudio19-dev python3-pip git
```

### 2. Clone the repo

```bash
git clone https://github.com/narrowkoala052010/Admute.git admute_light
cd admute_light
```

### 3. Python environment

```bash
python3 -m venv venvmute
source venvmute/bin/activate
pip install -r requirements.txt
```

### 4. Initialise the database

```bash
python db.py
```

### 5. Run

```bash
python daemon.py
```

The web UI is available at `http://<your-pi-ip>:5001`. The API secret is printed to the console on first run — save it, you'll need it to authenticate the UI.

---

## 🚀 Run as a service (auto-start on boot)

```bash
sudo cp admute-light.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable admute-light
sudo systemctl start admute-light
```

Follow logs:
```bash
journalctl -u admute-light -f
```

---

## 🎯 Recording your first ad

1. Open the web UI on your phone (`http://<pi-ip>:5001`)
2. When a commercial break starts, tap **Record**
3. When it ends, tap **Stop**
4. In the Recordings tab, review and tap **Ingest → Vault**
5. The next time that ad airs, AdMute Light will mute it automatically

The more ads you record, the more effective it becomes. Ads are stored as mathematical hashes — not audio — so the vault is tiny and fast.

---


## 📁 Project structure

```
admute_light/
├── daemon.py              # Process orchestrator
├── audio_capture.py       # P1: I2S microphone capture
├── fingerprint_worker.py  # P2: Acoustic fingerprint generation
├── match_engine.py        # P3: Tiered cache matching engine
├── mute_controller.py     # P4: HDMI-CEC mute control
├── api.py                 # P5: REST API + web UI server
├── config.py              # Configuration loader
├── config.toml            # User-editable settings
├── db.py                  # Database manager + migrations
├── matcher.py             # Shared time-coherence scoring
├── signal_utils.py        # DSP utilities (filter, gain)
├── bus.py                 # ZMQ socket address constants
├── console.py             # Logging setup
├── migrations/            # SQLite schema migrations
│   └── 001_initial_schema.sql
├── static/                # Web UI (HTML/CSS/JS)
└── requirements.txt
```

---

## 🤝 Contributing

Pull requests welcome. If you've built this and have a working vault of fingerprints for your region, consider sharing the `admute.db` file (hashes only — no audio, no personal data) to help build a community vault.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.