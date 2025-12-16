"""Discover YouTube and Wikipedia links for songs so the scrapers can run without
hand-prepared JSON inputs.

The YouTube lookup mirrors the fallback search used in audio_metadata_enrichment:
we run a lightweight yt-dlp search (`ytsearch1`) and capture the first result's
video ID/URL without downloading media. Wikipedia links are resolved via the
MediaWiki search API with simple heuristics favoring "(song)" titles.
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd
import requests
import yt_dlp
from tqdm import tqdm


DEFAULT_INPUT = Path("data") / "all_songs.csv"
DEFAULT_YOUTUBE_OUTPUT = Path("data") / "youtube_links.json"
DEFAULT_WIKI_OUTPUT = Path("data") / "wiki_awards_links.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="CSV or JSON dataset with name/artist columns.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N rows (after offset).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip the first N rows (useful for chunked runs).",
    )
    parser.add_argument(
        "--youtube-output",
        type=Path,
        default=DEFAULT_YOUTUBE_OUTPUT,
        help="Where to write discovered YouTube links (JSON list).",
    )
    parser.add_argument(
        "--wiki-output",
        type=Path,
        default=DEFAULT_WIKI_OUTPUT,
        help="Where to write discovered Wikipedia links (JSON list).",
    )
    parser.add_argument(
        "--skip-youtube",
        action="store_true",
        help="Do not perform YouTube searches.",
    )
    parser.add_argument(
        "--skip-wiki",
        action="store_true",
        help="Do not perform Wikipedia searches.",
    )
    parser.add_argument(
        "--force-wiki-search",
        action="store_true",
        help="Ignore existing 'link' column and always search Wikipedia.",
    )
    parser.add_argument(
        "--force-youtube-search",
        action="store_true",
        help="Ignore any existing YouTube URLs (e.g., audio_preview_url) and always search.",
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


def normalize(text: str) -> str:
    text_norm = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", text_norm.lower()).strip()


def extract_youtube_id(url: str) -> Optional[str]:
    match = re.search(r"v=([A-Za-z0-9_-]{6,})", url)
    return match.group(1) if match else None


def search_youtube_videos(query: str, top_n: int = 4) -> List[Tuple[str, str]]:
    """Return up to top_n (video_id, url) search results."""
    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "default_search": f"ytsearch{top_n}",
        "noplaylist": True,
        "skip_download": True,
        "retries": 1,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(query, download=False)

    # ytsearchN returns a playlist-like container with entries
    entries = info.get("entries") if info.get("_type") == "playlist" else [info]
    results: List[Tuple[str, str]] = []
    for entry in entries or []:
        video_id = entry.get("id") or extract_youtube_id(entry.get("webpage_url", "") or entry.get("url", ""))
        if not video_id:
            continue
        url = entry.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}"
        results.append((video_id, url))
        if len(results) >= top_n:
            break
    return results


def search_wikipedia_page(title: str, artist: str) -> Optional[str]:
    query = f"{title} {artist} song".strip()
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srlimit": 5,
        "srsearch": query,
    }
    resp = requests.get("https://en.wikipedia.org/w/api.php", params=params, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("query", {}).get("search", []) or []
    if not results:
        return None

    def score(result: Dict) -> float:
        title_norm = normalize(result.get("title", ""))
        snippet_norm = normalize(result.get("snippet", ""))
        target = normalize(title)
        artist_norm = normalize(artist)
        s = 0.0
        if "(song" in result.get("title", "").lower():
            s += 2.5
        if target and target in title_norm:
            s += 3.0
        if artist_norm and artist_norm in title_norm:
            s += 1.0
        if artist_norm and artist_norm in snippet_norm:
            s += 0.5
        return s

    best = max(results, key=score)
    best_title = best.get("title")
    if not best_title:
        return None
    return f"https://en.wikipedia.org/wiki/{quote(best_title.replace(' ', '_'))}"


def save_json(records: List[Dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(records, handle, indent=2, ensure_ascii=False)


def main() -> None:
    args = parse_args()
    df = load_dataset(args.input)
    df_slice = df.iloc[args.offset :]
    if args.limit:
        df_slice = df_slice.head(args.limit)

    youtube_records: List[Dict] = []
    wiki_records: List[Dict] = []

    iterator = tqdm(df_slice.iterrows(), total=len(df_slice), desc="Discovering links", unit="song")
    for _, row in iterator:
        name = row.get("name") or row.get("track_name")
        artist = row.get("artist")
        year = row.get("year")
        if not name or not artist:
            continue

        if not args.skip_wiki:
            wiki_link = row.get("link") if not args.force_wiki_search else None
            if not wiki_link:
                try:
                    wiki_link = search_wikipedia_page(str(name), str(artist))
                except Exception:
                    wiki_link = None
            if wiki_link:
                wiki_records.append(
                    {"name": name, "artist": artist, "year": year, "link": wiki_link}
                )

        if not args.skip_youtube:
            youtube_url = None
            youtube_id = None
            youtube_candidates: List[Dict[str, str]] = []
            if not args.force_youtube_search:
                youtube_url = row.get("youtube_url")
                # Reuse audio_metadata_enrichment fallback if present
                if not youtube_url and str(row.get("audio_metadata_source", "")).lower() == "youtube_search":
                    youtube_url = row.get("audio_preview_url")
                youtube_id = extract_youtube_id(youtube_url or "")

            if youtube_id:
                youtube_candidates.append(
                    {
                        "youtube_id": youtube_id,
                        "youtube_url": youtube_url or f"https://www.youtube.com/watch?v={youtube_id}",
                    }
                )

            if not youtube_id:
                try:
                    query = f"{name} {artist}".strip()
                    results = search_youtube_videos(query, top_n=4)
                    for vid, url in results:
                        youtube_candidates.append({"youtube_id": vid, "youtube_url": url})
                    if youtube_candidates:
                        youtube_id = youtube_candidates[0]["youtube_id"]
                        youtube_url = youtube_candidates[0]["youtube_url"]
                except Exception:
                    youtube_id = None
                    youtube_url = None

            if youtube_candidates:
                youtube_records.append(
                    {
                        "youtube_id": youtube_candidates[0]["youtube_id"],
                        "youtube_url": youtube_candidates[0].get("youtube_url")
                        or f"https://www.youtube.com/watch?v={youtube_candidates[0]['youtube_id']}",
                        "youtube_candidates": youtube_candidates,
                        "name": name,
                        "artist": artist,
                        "year": year,
                    }
                )

    if not args.skip_wiki:
        save_json(wiki_records, args.wiki_output)
        print(f"Saved {len(wiki_records)} Wikipedia links to {args.wiki_output}")
    if not args.skip_youtube:
        save_json(youtube_records, args.youtube_output)
        print(f"Saved {len(youtube_records)} YouTube links to {args.youtube_output}")


if __name__ == "__main__":
    main()
