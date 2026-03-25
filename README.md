# AdMute v4 🎯

**Open-source, hardware-accelerated acoustic automation for the living room.**

Modern Smart TV ecosystems and Server-Side Ad Insertion (SSAI) have fundamentally broken traditional network-level blockers (like Pi-hole) and browser extensions. You cannot install an ad-blocker on a native Roku or WebOS app, and blocking the DNS often breaks the video player entirely. 

AdMute takes back control by abandoning the network layer and operating entirely across the **analog air-gap**. 

It is a localized signal intelligence appliance that runs on a Raspberry Pi. It listens to the ambient audio in your room, generates real-time Shazam-style acoustic fingerprints, and instantly fires an Infrared (IR) command to mute your TV when it recognizes a commercial. When the ad pod ends, it unmutes. 

You own the TV, you own the remote, and you own the air in your room. AdMute simply automates the "Mute" button.

## 🛡️ Privacy & Security First
* **100% Local:** No cloud APIs. No telemetry. No accounts. AdMute runs entirely on your own Raspberry Pi. 
* **Analog Air-Gap:** Because it triggers via physical IR or CEC, streaming apps are entirely blind to the automation. You maintain your privacy without triggering anti-adblock scripts.
* **Cryptographic API:** The local React/Vanilla UI communicates with the Pi via an `aiohttp` server secured by strict HMAC-SHA256 signatures and 60-second replay-attack protection.

## 🏗️ The Microservice Architecture
AdMute v4 is built for uninterrupted Digital Signal Processing (DSP). It abandons monolithic Python threading for a true distributed system, utilizing **ZeroMQ (ZMQ)** to pass data at RAM-speed between dedicated CPU cores.

* **AudioCapture (Core 1):** Uninterrupted I2S digital microphone listener with DC-offset pedestal erasure. Pushes raw 2-second audio chunks to the ZMQ bus. Never drops a frame.
* **FingerprintWorkers (Cores 2 & 3):** Parallel, load-balanced math engines. Applies a 4th-order Butterworth bandpass filter and calculates Short-Time Fourier Transform (STFT) combinatorial hashes. Uses frequency quantization to survive real-world room distortion and TV speaker EQ shifts.
* **MatchEngine (Core 4):** The detective. Queries the SQLite WAL database and plots a Time-Coherence Histogram to guarantee matches without false positives.
* **MuteController:** The hardware trigger. Manages temporal shielding (debounce) and fires LIRC `irsend` commands.

## 🤝 The "Ad-Hunter" Initiative
Acoustic fingerprinting is only as good as the database it checks against. We are building a decentralized, community-driven Vault of commercial fingerprints. 

Because AdMute only extracts and stores mathematical hashes—**not copyrighted audio files**—sharing your Vault database is legally sterile and incredibly lightweight. 

**Call to Action:** We need technical users to run the daemon, record the ads in their region, and contribute their SQLite hash tables to the master repository. Together, we can map the acoustic footprint of the entire ad-tech industry.

## 🛠️ Setup & Deployment
*(Detailed hardware wiring for I2S INMP441 microphones, LIRC setup, and systemd daemon deployment coming soon).*
