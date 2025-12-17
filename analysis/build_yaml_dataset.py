"""Assemble the enriched dataset into a YAML document."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import yaml

from audio_features import AUDIO_METADATA_COLUMNS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data") / "songs_database.json",
        help="Primary dataset (JSON or CSV).",
    )
    parser.add_argument(
        "--spotify-features",
        type=Path,
        default=Path("data") / "songs_with_audio_features.csv",
        help="Optional Spotify audio feature CSV.",
    )
    parser.add_argument(
        "--audio-metadata",
        type=Path,
        default=Path("data") / "songs_with_audio_metadata.json",
        help="Optional file containing librosa audio metadata.",
    )
    parser.add_argument(
        "--youtube-comments",
        type=Path,
        default=Path("data") / "youtube_comments.json",
        help="Optional scraped comments JSON from the YouTube spider.",
    )
    parser.add_argument(
        "--youtube-links",
        type=Path,
        default=Path("data") / "youtube_links.json",
        help="Optional YouTube discovery output to attach IDs/URLs.",
    )
    parser.add_argument(
        "--awards",
        type=Path,
        default=Path("data") / "wikipedia_awards.json",
        help="Optional scraped awards JSON from the Wikipedia spider.",
    )
    parser.add_argument(
        "--comment-limit",
        type=int,
        default=10,
        help="Maximum number of comments per track to keep (capped at 15).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data") / "songs_dataset.yaml",
        help="Destination YAML path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only include the first N rows (useful for sampling).",
    )
    return parser.parse_args()


def load_records(path: Path) -> List[Dict]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found at {path}")
    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    df = pd.read_csv(path)
    return df.to_dict(orient="records")


def load_optional_records(path: Path) -> List[Dict]:
    return load_records(path) if path.exists() else []


def make_track_key(name: str, artist: str) -> str:
    safe_name = (name or "").strip()
    safe_artist = (artist or "").strip()
    return f"{safe_name}|{safe_artist}".lower()


def to_native(value):
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def build_audio_metadata_map(records: List[Dict]) -> Dict[str, Dict[str, float]]:
    mapping: Dict[str, Dict[str, float]] = {}
    for row in records:
        track_id = row.get("spotify_track_id")
        metadata = row.get("audio_metadata")
        if not track_id or not isinstance(metadata, dict):
            continue
        filtered = {key: metadata.get(key) for key in AUDIO_METADATA_COLUMNS}
        mapping[str(track_id)] = filtered
    return mapping


def build_spotify_feature_map(records: List[Dict]) -> Dict[str, Dict[str, object]]:
    if not records:
        return {}
    df = pd.DataFrame(records)
    if "spotify_track_id" not in df.columns:
        return {}

    base_cols = {"year", "place", "name", "artist", "link", "popularity"}
    feature_cols = [c for c in df.columns if c not in base_cols]

    feature_map: Dict[str, Dict[str, object]] = {}
    for row in df.to_dict(orient="records"):
        track_id = row.get("spotify_track_id")
        if not track_id:
            continue
        feature_map[str(track_id)] = {col: to_native(row.get(col)) for col in feature_cols}
    return feature_map


def build_comment_map(records: List[Dict]) -> Dict[str, List[Dict]]:
    mapping: Dict[str, List[Dict]] = {}
    for row in records:
        youtube_id = row.get("youtube_id")
        track_key = row.get("track_key") or make_track_key(
            row.get("track_name", "") or row.get("name", ""),
            row.get("artist", ""),
        )
        keys = [key for key in (youtube_id, track_key) if key]
        if not keys:
            continue

        entry = {
            "comment_id": row.get("comment_id"),
            "author": row.get("author"),
            "text": row.get("text"),
            "like_count": to_native(row.get("like_count")),
            "published_at": row.get("published_at"),
            "position": to_native(row.get("position")),
        }
        for key in keys:
            mapping.setdefault(str(key), []).append(entry)
    for comments in mapping.values():
        comments.sort(key=lambda c: (c.get("position") or 0))
    return mapping


def build_youtube_link_map(records: List[Dict]) -> Dict[str, Dict[str, object]]:
    mapping: Dict[str, Dict[str, object]] = {}
    for row in records:
        key = make_track_key(row.get("name", "") or row.get("track_name", ""), row.get("artist", ""))
        if not key:
            continue
        youtube_id = row.get("youtube_id")
        youtube_url = row.get("youtube_url")
        if not youtube_url and youtube_id:
            youtube_url = f"https://www.youtube.com/watch?v={youtube_id}"
        mapping[key] = {
            "youtube_id": youtube_id,
            "youtube_url": youtube_url,
            "youtube_candidates": row.get("youtube_candidates") or [],
        }
    return mapping


def merge_youtube_links(base_records: List[Dict], link_map: Dict[str, Dict[str, object]]) -> None:
    for row in base_records:
        track_key = make_track_key(row.get("name", ""), row.get("artist", ""))
        link = link_map.get(track_key)
        if not link:
            continue
        row["youtube_id"] = row.get("youtube_id") or link.get("youtube_id")
        row["youtube_url"] = row.get("youtube_url") or link.get("youtube_url")
        if link.get("youtube_candidates"):
            row["youtube_candidates"] = link.get("youtube_candidates")


def build_award_map(records: List[Dict]) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    for row in records:
        key = (
            row.get("track_key")
            or make_track_key(row.get("track_name", ""), row.get("artist", ""))
        )
        if not key:
            continue
        awards = row.get("awards") or []
        mapping[key] = [award for award in awards if award]
    return mapping


def merge_audio_metadata(base_records: List[Dict], metadata_map: Dict[str, Dict]) -> None:
    for row in base_records:
        track_id = str(row.get("spotify_track_id")) if row.get("spotify_track_id") else None
        if track_id and track_id in metadata_map:
            row["audio_metadata"] = metadata_map[track_id]


def merge_comments(base_records: List[Dict], comment_map: Dict[str, List[Dict]], limit: int = 10) -> None:
    capped_limit = min(max(limit, 0), 15)
    for row in base_records:
        youtube_key = row.get("youtube_id")
        track_key = make_track_key(row.get("name", ""), row.get("artist", ""))
        comments = comment_map.get(youtube_key) or comment_map.get(track_key) or []
        row["youtube_comments"] = comments[:capped_limit]


def merge_awards(base_records: List[Dict], award_map: Dict[str, List[str]]) -> None:
    for row in base_records:
        track_key = make_track_key(row.get("name", ""), row.get("artist", ""))
        row["awards"] = award_map.get(track_key, [])


def merge_spotify_features(base_records: List[Dict], feature_map: Dict[str, Dict]) -> None:
    for row in base_records:
        track_id = str(row.get("spotify_track_id")) if row.get("spotify_track_id") else None
        if track_id and track_id in feature_map:
            row["spotify_audio_features"] = feature_map[track_id]


def clean_record(row: Dict) -> Dict:
    cleaned: Dict = {
        "name": row.get("name"),
        "artist": row.get("artist"),
        "year": to_native(row.get("year")),
        "rank": row.get("place"),
        "wikipedia_link": row.get("link"),
        "spotify_track_id": row.get("spotify_track_id"),
        "popularity": to_native(row.get("popularity")),
        "lyrics": {
            "text": row.get("lyrics"),
            "status": row.get("genius_status"),
            "pageviews": to_native(row.get("genius_pageviews")),
            "release_date": row.get("genius_release_date"),
        },
        "audio_metadata": row.get("audio_metadata"),
        "spotify_audio_features": row.get("spotify_audio_features"),
        "audio_preview_url": row.get("audio_preview_url"),
        "youtube_id": row.get("youtube_id"),
        "youtube_url": row.get("youtube_url"),
        "youtube_comments": row.get("youtube_comments", []),
        "awards": row.get("awards", []),
    }
    return cleaned


def main() -> None:
    args = parse_args()
    base_records = load_records(args.input)
    if args.limit:
        base_records = base_records[: args.limit]

    audio_metadata_records = load_optional_records(args.audio_metadata)
    spotify_feature_records = load_optional_records(args.spotify_features)
    youtube_link_records = load_optional_records(args.youtube_links)
    comment_records = load_optional_records(args.youtube_comments)
    award_records = load_optional_records(args.awards)

    merge_audio_metadata(base_records, build_audio_metadata_map(audio_metadata_records))
    merge_spotify_features(base_records, build_spotify_feature_map(spotify_feature_records))
    merge_youtube_links(base_records, build_youtube_link_map(youtube_link_records))
    merge_comments(base_records, build_comment_map(comment_records), limit=args.comment_limit)
    merge_awards(base_records, build_award_map(award_records))

    cleaned = [clean_record(row) for row in base_records]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            cleaned,
            handle,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )
    print(f"Wrote YAML dataset to {args.output}")


if __name__ == "__main__":
    main()
