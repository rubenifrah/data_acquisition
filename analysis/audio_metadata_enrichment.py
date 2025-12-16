"""Compute rich audio metadata for each track using Spotify preview clips.

The feature computation mirrors the scratch ``youtube_feature_extractor.py`` to
keep parity with the earlier neural-net experiments while embedding the results
into our dataset.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yt_dlp
import librosa
from dotenv import load_dotenv
from tqdm import tqdm
import warnings

from audio_features import AUDIO_METADATA_COLUMNS, compute_audio_features
from spotify_enrichment import get_spotify_client


DEFAULT_INPUT = Path("data") / "songs_database.json"
DEFAULT_OUTPUT = Path("data") / "songs_with_audio_metadata.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Input JSON or CSV dataset (defaults to songs_database.json).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Where to write the enriched dataset.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N rows (useful for testing).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Maximum clip duration in seconds.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Row offset to start processing from (useful for chunking).",
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=22050,
        help="Target sample rate for librosa.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute metadata even if it already exists on a row.",
    )
    return parser.parse_args()


def load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found at {path}")

    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return pd.DataFrame(data)
    return pd.read_csv(path)


def save_dataset(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = df.to_dict(orient="records")
    with path.open("w", encoding="utf-8") as handle:
        json.dump(records, handle, indent=2, ensure_ascii=False)


def fetch_preview_urls(sp, track_ids: List[str]) -> Dict[str, Optional[str]]:
    preview_map: Dict[str, Optional[str]] = {}
    batch_size = 50
    for i in tqdm(range(0, len(track_ids), batch_size), desc="Fetching Spotify previews", unit="batch"):
        batch = track_ids[i : i + batch_size]
        response = sp.tracks(batch)
        for track in response.get("tracks", []):
            track_id = track.get("id")
            preview_map[track_id] = track.get("preview_url") if track_id else None
    return preview_map


def download_youtube_samples(
    query: str, sample_rate: int, duration: float, max_attempts: int = 3
) -> Tuple[np.ndarray, str]:
    """Search YouTube for the query, download best audio, and return samples + URL."""
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                temp_dir = Path(tmpdir)
                ydl_opts = {
                    "format": "bestaudio/best",
                    "quiet": True,
                    "no_warnings": True,
                    "default_search": "ytsearch1",
                    "noplaylist": True,
                    "outtmpl": str(temp_dir / "%(id)s.%(ext)s"),
                    "retries": 0,
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(query, download=True)
                    if info.get("_type") == "playlist":
                        entries = info.get("entries") or []
                        info = next((entry for entry in entries if entry), None) or info
                    downloaded = Path(ydl.prepare_filename(info))
                    source_url = info.get("webpage_url") or info.get("url") or ""

                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="PySoundFile failed.*", category=UserWarning)
                    warnings.filterwarnings(
                        "ignore",
                        message="librosa.core.audio.__audioread_load",
                        category=FutureWarning,
                    )
                    samples, _ = librosa.load(downloaded, sr=sample_rate, duration=duration)
                if samples.size == 0:
                    raise RuntimeError("Loaded YouTube clip is empty")
                return samples, source_url
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
    raise last_error if last_error else RuntimeError("YouTube download failed")


def download_preview_clip(url: str, timeout: float = 20.0) -> bytes:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def load_samples_from_bytes(
    payload: bytes, sample_rate: int, duration: float
) -> Optional["np.ndarray"]:
    import librosa
    import numpy as np

    with tempfile.NamedTemporaryFile(suffix=".mp3") as tmp:
        tmp.write(payload)
        tmp.flush()
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="PySoundFile failed.*", category=UserWarning)
            warnings.filterwarnings(
                "ignore",
                message="librosa.core.audio.__audioread_load",
                category=FutureWarning,
            )
            samples, _ = librosa.load(tmp.name, sr=sample_rate, duration=duration)
    if samples.size == 0:
        return None
    return samples


def should_skip_row(row: pd.Series, force: bool) -> bool:
    if force:
        return False
    existing = row.get("audio_metadata")
    return isinstance(existing, dict) and bool(existing)


def enrich_audio_metadata(
    df: pd.DataFrame,
    sp,
    sample_rate: int,
    duration: float,
    limit: Optional[int],
    offset: int,
    force: bool,
) -> pd.DataFrame:
    df_slice = df.iloc[offset:]
    candidate_mask = df_slice["spotify_track_id"].notna()
    to_process = df_slice[candidate_mask]
    if limit:
        to_process = to_process.head(limit)

    track_ids = to_process["spotify_track_id"].astype(str).tolist()
    preview_urls = fetch_preview_urls(sp, track_ids)

    progress = tqdm(
        to_process.iterrows(),
        total=len(to_process),
        desc="Computing audio metadata",
        unit="track",
    )
    for index, row in progress:
        track_id = str(row["spotify_track_id"])
        if should_skip_row(row, force):
            continue

        preview_url = preview_urls.get(track_id)
        metadata = None
        source_url = None
        source_label = None
        status_text = None

        if preview_url:
            try:
                payload = download_preview_clip(preview_url)
                samples = load_samples_from_bytes(payload, sample_rate, duration)
                if samples is not None:
                    metadata_raw = compute_audio_features(samples, sample_rate)
                    metadata = {key: metadata_raw.get(key) for key in AUDIO_METADATA_COLUMNS}
                    source_url = preview_url
                    source_label = "spotify_preview"
                    status_text = "ok (preview)"
                else:
                    status_text = "empty audio from preview"
            except Exception as exc:
                status_text = f"preview error: {exc}"

        if metadata is None:
            try:
                query = f"{row.get('name', '')} {row.get('artist', '')}".strip()
                samples, youtube_url = download_youtube_samples(query, sample_rate, duration)
                metadata_raw = compute_audio_features(samples, sample_rate)
                metadata = {key: metadata_raw.get(key) for key in AUDIO_METADATA_COLUMNS}
                source_url = youtube_url
                source_label = "youtube_search"
                status_text = "ok (youtube)"
            except Exception as exc:
                if not status_text:
                    status_text = f"error: {exc}"

        df.at[index, "audio_metadata"] = metadata
        df.at[index, "audio_metadata_status"] = status_text
        df.at[index, "audio_preview_url"] = source_url
        df.at[index, "audio_metadata_source"] = source_label

    return df


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")
    args = parse_args()
    df = load_dataset(args.input)

    if "spotify_track_id" not in df.columns:
        raise ValueError("Dataset must contain a 'spotify_track_id' column.")

    if "audio_metadata_status" not in df.columns:
        df["audio_metadata_status"] = None
    if "audio_metadata" not in df.columns:
        df["audio_metadata"] = None
    if "audio_preview_url" not in df.columns:
        df["audio_preview_url"] = None
    if "audio_metadata_source" not in df.columns:
        df["audio_metadata_source"] = None

    sp = get_spotify_client()
    if sp is None:
        raise RuntimeError("Spotify client could not be initialized.")

    enriched = enrich_audio_metadata(
        df=df,
        sp=sp,
        sample_rate=args.sample_rate,
        duration=args.duration,
        limit=args.limit,
        offset=args.offset,
        force=args.force,
    )
    save_dataset(enriched, args.output)
    print(f"Saved audio-enriched dataset to {args.output}")


if __name__ == "__main__":
    main()
