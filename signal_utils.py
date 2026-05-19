"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
DSP utilities — shared bandpass filter builder and digital gain
function used consistently across AudioCapture, FingerprintWorker,
and the API ingest pipeline.
"""

import numpy as np
from scipy.signal import butter, lfilter

def build_bandpass(lowcut=300.0, highcut=16000.0, fs=44100, order=5):
    """Creates the Human-Range filter coefficients."""
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = butter(order, [low, high], btype='band')
    return b, a

def apply_digital_gain(raw_pcm: np.ndarray, gain: float = 25.0) -> tuple[np.ndarray, float]:
    """Applies the Digital Pre-Amp to the raw I2S data."""
    # Ensure even length for stereo-to-mono conversion
    if len(raw_pcm) % 2 != 0:
        raw_pcm = raw_pcm[:-1]
    
    # Convert 32-bit integer PCM to float64 in range [-1.0, 1.0]
    audio = raw_pcm.astype(np.float64) / 2_147_483_648.0
    
    # Extract left channel (mono) and remove DC offset
    mono = audio[0::2].copy()
    mono -= np.mean(mono)
    
    # Apply the Gain (e.g., 25x) and apply the invisible "brick wall" (clip)
    # This prevents the FFT from crashing if a loud noise spikes above 1.0
    mono = np.clip(mono * gain, -1.0, 1.0)
    
    # Calculate the new, amplified peak
    peak = float(np.max(np.abs(mono)))
    
    return mono.astype(np.float32), peak

def apply_filter(audio_data: np.ndarray, b, a) -> np.ndarray:
    """Applies the band-pass filter to remove fog and rumble."""
    return lfilter(b, a, audio_data).astype(np.float32)