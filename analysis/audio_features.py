"""Audio feature extraction helpers based on the interactive music classifier utilities.

The functions mirror the feature computation used in
``scratch_neural_nets/music/youtube_feature_extractor.py`` so we can reuse the
same metadata in this project without pulling that repository as a dependency.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import librosa
import numpy as np

try:  # Optional rhythm API availability varies with librosa versions
    from librosa.feature import rhythm as _librosa_rhythm  # type: ignore
except Exception:  # pragma: no cover - fallback if the submodule is missing
    _librosa_rhythm = None


# Full header used by the original extractor; keep order stable for CSV/YAML output.
FEATURE_HEADER: List[str] = [
    "filename",
    "length",
    "chroma_stft_mean",
    "chroma_stft_var",
    "rms_mean",
    "rms_var",
    "spectral_centroid_mean",
    "spectral_centroid_var",
    "spectral_bandwidth_mean",
    "spectral_bandwidth_var",
    "rolloff_mean",
    "rolloff_var",
    "zero_crossing_rate_mean",
    "zero_crossing_rate_var",
    "harmony_mean",
    "harmony_var",
    "perceptr_mean",
    "perceptr_var",
    "tempo",
    "mfcc1_mean",
    "mfcc1_var",
    "mfcc2_mean",
    "mfcc2_var",
    "mfcc3_mean",
    "mfcc3_var",
    "mfcc4_mean",
    "mfcc4_var",
    "mfcc5_mean",
    "mfcc5_var",
    "mfcc6_mean",
    "mfcc6_var",
    "mfcc7_mean",
    "mfcc7_var",
    "mfcc8_mean",
    "mfcc8_var",
    "mfcc9_mean",
    "mfcc9_var",
    "mfcc10_mean",
    "mfcc10_var",
    "mfcc11_mean",
    "mfcc11_var",
    "mfcc12_mean",
    "mfcc12_var",
    "mfcc13_mean",
    "mfcc13_var",
    "mfcc14_mean",
    "mfcc14_var",
    "mfcc15_mean",
    "mfcc15_var",
    "mfcc16_mean",
    "mfcc16_var",
    "mfcc17_mean",
    "mfcc17_var",
    "mfcc18_mean",
    "mfcc18_var",
    "mfcc19_mean",
    "mfcc19_var",
    "mfcc20_mean",
    "mfcc20_var",
    "label",
]

# Fields we keep for metadata storage (drop file name/label)
AUDIO_METADATA_COLUMNS: List[str] = [
    column for column in FEATURE_HEADER if column not in {"filename", "label"}
]


@dataclass
class AudioMetadata:
    """Structured container for the computed librosa features."""

    length: int
    chroma_stft_mean: float
    chroma_stft_var: float
    rms_mean: float
    rms_var: float
    spectral_centroid_mean: float
    spectral_centroid_var: float
    spectral_bandwidth_mean: float
    spectral_bandwidth_var: float
    rolloff_mean: float
    rolloff_var: float
    zero_crossing_rate_mean: float
    zero_crossing_rate_var: float
    harmony_mean: float
    harmony_var: float
    perceptr_mean: float
    perceptr_var: float
    tempo: float
    mfcc1_mean: float
    mfcc1_var: float
    mfcc2_mean: float
    mfcc2_var: float
    mfcc3_mean: float
    mfcc3_var: float
    mfcc4_mean: float
    mfcc4_var: float
    mfcc5_mean: float
    mfcc5_var: float
    mfcc6_mean: float
    mfcc6_var: float
    mfcc7_mean: float
    mfcc7_var: float
    mfcc8_mean: float
    mfcc8_var: float
    mfcc9_mean: float
    mfcc9_var: float
    mfcc10_mean: float
    mfcc10_var: float
    mfcc11_mean: float
    mfcc11_var: float
    mfcc12_mean: float
    mfcc12_var: float
    mfcc13_mean: float
    mfcc13_var: float
    mfcc14_mean: float
    mfcc14_var: float
    mfcc15_mean: float
    mfcc15_var: float
    mfcc16_mean: float
    mfcc16_var: float
    mfcc17_mean: float
    mfcc17_var: float
    mfcc18_mean: float
    mfcc18_var: float
    mfcc19_mean: float
    mfcc19_var: float
    mfcc20_mean: float
    mfcc20_var: float

    def to_dict(self) -> Dict[str, float]:
        return {field: getattr(self, field) for field in AUDIO_METADATA_COLUMNS}


def _mean_var(feature: np.ndarray) -> np.ndarray:
    flat = np.ravel(feature)
    return np.array([float(np.mean(flat)), float(np.var(flat))])


def compute_audio_features(samples: np.ndarray, sr: int) -> Dict[str, float]:
    """Compute the librosa-based metadata dictionary for a clip."""
    metadata: Dict[str, float] = {
        "length": int(samples.size),
    }

    chroma = librosa.feature.chroma_stft(y=samples, sr=sr)
    metadata["chroma_stft_mean"], metadata["chroma_stft_var"] = _mean_var(chroma)

    rms = librosa.feature.rms(y=samples)
    metadata["rms_mean"], metadata["rms_var"] = _mean_var(rms)

    spec_centroid = librosa.feature.spectral_centroid(y=samples, sr=sr)
    metadata["spectral_centroid_mean"], metadata["spectral_centroid_var"] = _mean_var(
        spec_centroid
    )

    spec_bandwidth = librosa.feature.spectral_bandwidth(y=samples, sr=sr)
    metadata["spectral_bandwidth_mean"], metadata["spectral_bandwidth_var"] = _mean_var(
        spec_bandwidth
    )

    rolloff = librosa.feature.spectral_rolloff(y=samples, sr=sr)
    metadata["rolloff_mean"], metadata["rolloff_var"] = _mean_var(rolloff)

    zcr = librosa.feature.zero_crossing_rate(samples)
    metadata["zero_crossing_rate_mean"], metadata["zero_crossing_rate_var"] = _mean_var(
        zcr
    )

    harmony = librosa.effects.harmonic(samples)
    metadata["harmony_mean"] = float(np.mean(harmony))
    metadata["harmony_var"] = float(np.var(harmony))

    percussive = librosa.effects.percussive(samples)
    metadata["perceptr_mean"] = float(np.mean(percussive))
    metadata["perceptr_var"] = float(np.var(percussive))

    if _librosa_rhythm is not None:
        tempo = _librosa_rhythm.tempo(y=samples, sr=sr)
    else:
        tempo = librosa.beat.tempo(y=samples, sr=sr)
    metadata["tempo"] = float(tempo[0] if tempo.size else 0.0)

    mfcc = librosa.feature.mfcc(y=samples, sr=sr, n_mfcc=20)
    for i in range(20):
        mean_val, var_val = _mean_var(mfcc[i])
        metadata[f"mfcc{i + 1}_mean"] = mean_val
        metadata[f"mfcc{i + 1}_var"] = var_val

    return metadata


def build_feature_row(
    samples: np.ndarray, sr: int, filename: Optional[str] = None, label: str = ""
) -> Dict[str, object]:
    """Return a row-shaped mapping aligned with ``FEATURE_HEADER``."""
    metadata = compute_audio_features(samples, sr)
    row: Dict[str, object] = {}
    if filename is not None:
        row["filename"] = filename
    row.update(metadata)
    row["label"] = label
    return row


def ordered_audio_metadata(row: Dict[str, object], header: Iterable[str]) -> List[object]:
    """Return an ordered list of feature values following the provided header."""
    return [row.get(key) for key in header]
