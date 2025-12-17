"""Run the end-to-end data pipeline with per-song pipelining.

Each song flows through the stages one by one (audio metadata -> YouTube link
discovery -> YouTube comments -> Wikipedia awards), and stages are overlapped
so audio for song N+1 can start as soon as song N moves to link discovery.
Intermediate JSONs are updated after every song, making long runs resilient to
interruptions while naturally pacing API calls.
"""
from __future__ import annotations

import argparse
import json
import queue
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from tqdm import tqdm

# Ensure local analysis modules (audio_features, build_yaml_dataset, etc.) are importable
ROOT = Path(__file__).resolve().parent
if str(ROOT / "analysis") not in sys.path:
    sys.path.insert(0, str(ROOT / "analysis"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis import build_yaml_dataset as assembler


DATA = ROOT / "data"

# Core paths
SPOTIFY_FEATURES_CSV = DATA / "songs_with_audio_features.csv"
SONGS_DB_JSON = DATA / "songs_database.json"
AUDIO_METADATA_JSON = DATA / "songs_with_audio_metadata.json"
YOUTUBE_LINKS_JSON = DATA / "youtube_links.json"
YOUTUBE_COMMENTS_JSON = DATA / "youtube_comments.json"
WIKI_AWARDS_JSON = DATA / "wikipedia_awards.json"
FINAL_YAML = DATA / "songs_dataset.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-n",
        type=int,
        required=True,
        help="Number of new fully processed songs to add to the YAML.",
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=22050,
        help="Sample rate for audio metadata computation.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Clip duration (seconds) for audio metadata computation.",
    )
    parser.add_argument(
        "--comments",
        type=int,
        default=10,
        help="Number of top comments to include per track (capped at 15).",
    )
    return parser.parse_args()


def run(cmd: List[str], cwd: Path | None = None, quiet: bool = False) -> None:
    printable = " ".join(cmd)
    if not quiet:
        print(f"\n$ {printable}")
    result = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE if quiet else None,
        stderr=subprocess.STDOUT if quiet else None,
    )
    if result.returncode != 0:
        if quiet and result.stdout:
            print(result.stdout)
        result.check_returncode()


def clamp_comment_limit(value: int) -> int:
    return min(max(value, 0), 15)


def load_yaml(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or []
    if not isinstance(data, list):
        return []
    return data


def load_json_list(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle) or []


def atomic_write_json(path: Path, records: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(records, handle, indent=2, ensure_ascii=False)
    tmp_path.replace(path)


def ensure_song_database() -> None:
    if not SONGS_DB_JSON.exists():
        raise FileNotFoundError(
            f"Missing base dataset at {SONGS_DB_JSON}. "
            "Run the Spotify/lyrics prep to build songs_database.json first."
        )


def run_audio_metadata(
    limit: int,
    sample_rate: int,
    duration: float,
    offset: int = 0,
    input_path: Path = SONGS_DB_JSON,
    output_path: Path = AUDIO_METADATA_JSON,
    force: bool = False,
) -> None:
    cmd = [
        sys.executable,
        str(ROOT / "analysis" / "audio_metadata_enrichment.py"),
        "--input",
        str(input_path),
        "--output",
        str(output_path),
        "--limit",
        str(limit),
        "--offset",
        str(offset),
        "--sample-rate",
        str(sample_rate),
        "--duration",
        str(duration),
    ]
    if force:
        cmd.append("--force")
    run(cmd, quiet=True)


def run_youtube_link_discovery(
    limit: int,
    offset: int = 0,
    input_path: Path = SONGS_DB_JSON,
    output_path: Path = YOUTUBE_LINKS_JSON,
    skip_wiki: bool = True,
) -> None:
    cmd = [
        sys.executable,
        str(ROOT / "analysis" / "discover_links.py"),
        "--input",
        str(input_path),
        "--limit",
        str(limit),
        "--offset",
        str(offset),
        "--youtube-output",
        str(output_path),
    ]
    if skip_wiki:
        cmd.append("--skip-wiki")
    run(cmd, quiet=True)


def run_youtube_comments(
    limit: int,
    comment_limit: int,
    links_path: Path = YOUTUBE_LINKS_JSON,
    output_path: Path = YOUTUBE_COMMENTS_JSON,
) -> None:
    run(
        [
            "scrapy",
            "crawl",
            "youtube_comments",
            "--nolog",
            "-a",
            f"links_path={links_path}",
            "-a",
            f"limit={limit}",
            "-a",
            f"max_comments={comment_limit}",
            "-O",
            str(output_path),
        ],
        cwd=ROOT / "billboard_scraper",
        quiet=True,
    )


def run_wikipedia_awards(
    limit: int,
    dataset_path: Path = SONGS_DB_JSON,
    output_path: Path = WIKI_AWARDS_JSON,
) -> None:
    run(
        [
            "scrapy",
            "crawl",
            "wikipedia_awards",
            "--nolog",
            "-a",
            f"dataset_path={dataset_path}",
            "-a",
            f"limit={limit}",
            "-O",
            str(output_path),
        ],
        cwd=ROOT / "billboard_scraper",
        quiet=True,
    )


def build_order_map(records: List[Dict]) -> Dict[str, int]:
    return {assembler.make_track_key(row.get("name", ""), row.get("artist", "")): idx for idx, row in enumerate(records)}


def make_key(record: Dict) -> str:
    return assembler.make_track_key(record.get("name", ""), record.get("artist", ""))


def is_complete(record: Dict) -> bool:
    has_spotify = bool(record.get("spotify_track_id"))
    has_audio = bool(record.get("audio_metadata"))
    return has_spotify and has_audio


def missing_fields(record: Dict) -> List[str]:
    missing: List[str] = []
    if not record.get("spotify_track_id"):
        missing.append("spotify_track_id")
    if not record.get("audio_metadata"):
        missing.append("audio_metadata")
    if not record.get("lyrics", {}).get("text"):
        missing.append("lyrics")
    return missing


@dataclass
class SongJob:
    index: int
    record: Dict
    track_key: str

    def label(self) -> str:
        return f"{self.record.get('name')} - {self.record.get('artist')}"


class SongPipeline:
    def __init__(
        self,
        base_records: List[Dict],
        order_map: Dict[str, int],
        comment_limit: int,
        sample_rate: int,
        duration: float,
    ):
        self.base_records = base_records
        self.order_map = order_map
        self.comment_limit = clamp_comment_limit(comment_limit)
        self.sample_rate = sample_rate
        self.duration = duration

    def _sorted_keys(self, keys: List[str]) -> List[str]:
        return sorted(keys, key=lambda k: self.order_map.get(k, len(self.order_map) + 1))

    def _merge_by_track_key(self, existing: List[Dict], new_records: List[Dict]) -> List[Dict]:
        merged: Dict[str, Dict] = {}
        for row in existing:
            key = assembler.make_track_key(row.get("name", "") or row.get("track_name", ""), row.get("artist", ""))
            if key:
                merged[key] = row
        for row in new_records:
            key = assembler.make_track_key(row.get("name", "") or row.get("track_name", ""), row.get("artist", ""))
            if key:
                merged[key] = row
        ordered = self._sorted_keys(list(merged.keys()))
        return [merged[k] for k in ordered]

    def _comment_matches(self, row: Dict, youtube_id: Optional[str], track_key: str) -> bool:
        rid = row.get("youtube_id")
        rkey = row.get("track_key") or assembler.make_track_key(
            row.get("track_name", "") or row.get("name", ""),
            row.get("artist", ""),
        )
        if youtube_id and rid and str(rid) == str(youtube_id):
            return True
        return bool(track_key) and rkey == track_key

    def process_audio_metadata(self, job: SongJob) -> None:
        track_id = job.record.get("spotify_track_id")
        if not track_id:
            return

        metadata_map = assembler.build_audio_metadata_map(
            assembler.load_optional_records(AUDIO_METADATA_JSON)
        )
        if str(track_id) in metadata_map and metadata_map[str(track_id)]:
            return

        input_path = AUDIO_METADATA_JSON if AUDIO_METADATA_JSON.exists() else SONGS_DB_JSON
        run_audio_metadata(
            limit=1,
            sample_rate=self.sample_rate,
            duration=self.duration,
            offset=job.index,
            input_path=input_path,
            output_path=AUDIO_METADATA_JSON,
        )

    def process_youtube_links(self, job: SongJob) -> None:
        link_map = assembler.build_youtube_link_map(
            assembler.load_optional_records(YOUTUBE_LINKS_JSON)
        )
        if job.track_key in link_map:
            return

        dataset_input = AUDIO_METADATA_JSON if AUDIO_METADATA_JSON.exists() else SONGS_DB_JSON
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            run_youtube_link_discovery(
                limit=1,
                offset=job.index,
                input_path=dataset_input,
                output_path=tmp_path,
            )
            new_links = load_json_list(tmp_path)
            if not new_links:
                return
            merged = self._merge_by_track_key(load_json_list(YOUTUBE_LINKS_JSON), new_links)
            atomic_write_json(YOUTUBE_LINKS_JSON, merged)
        finally:
            tmp_path.unlink(missing_ok=True)

    def process_youtube_comments(self, job: SongJob) -> None:
        link_map = assembler.build_youtube_link_map(
            assembler.load_optional_records(YOUTUBE_LINKS_JSON)
        )
        link = link_map.get(job.track_key)
        youtube_id = link.get("youtube_id") if link else None
        if not link or not youtube_id:
            return

        comment_map = assembler.build_comment_map(
            assembler.load_optional_records(YOUTUBE_COMMENTS_JSON)
        )
        existing_comments = comment_map.get(youtube_id) or comment_map.get(job.track_key) or []
        if len(existing_comments) >= self.comment_limit:
            return

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp_links, tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False
        ) as tmp_comments:
            link_payload = dict(link)
            link_payload.setdefault("name", job.record.get("name"))
            link_payload.setdefault("artist", job.record.get("artist"))
            json.dump([link_payload], tmp_links, indent=2, ensure_ascii=False)
            tmp_links.flush()
            tmp_path_links = Path(tmp_links.name)
            tmp_path_comments = Path(tmp_comments.name)

        try:
            run_youtube_comments(
                limit=1,
                comment_limit=self.comment_limit,
                links_path=tmp_path_links,
                output_path=tmp_path_comments,
            )
            new_comments = load_json_list(tmp_path_comments)
            if not new_comments:
                return

            for row in new_comments:
                row.setdefault("track_key", job.track_key)
                row.setdefault("track_name", job.record.get("name"))
                row.setdefault("artist", job.record.get("artist"))
                row.setdefault("youtube_id", youtube_id)

            existing_rows = load_json_list(YOUTUBE_COMMENTS_JSON)
            filtered = [
                row for row in existing_rows if not self._comment_matches(row, youtube_id, job.track_key)
            ]
            merged = filtered + new_comments
            atomic_write_json(YOUTUBE_COMMENTS_JSON, merged)
        finally:
            tmp_path_links.unlink(missing_ok=True)
            tmp_path_comments.unlink(missing_ok=True)

    def process_wikipedia_awards(self, job: SongJob) -> None:
        link = job.record.get("link") or job.record.get("wikipedia_link")
        if not link:
            return

        award_map = assembler.build_award_map(
            assembler.load_optional_records(WIKI_AWARDS_JSON)
        )
        if award_map.get(job.track_key):
            return

        entry = {
            "name": job.record.get("name"),
            "artist": job.record.get("artist"),
            "year": job.record.get("year"),
            "link": link,
        }

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp_dataset, tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False
        ) as tmp_output:
            json.dump([entry], tmp_dataset, indent=2, ensure_ascii=False)
            tmp_dataset.flush()
            dataset_path = Path(tmp_dataset.name)
            output_path = Path(tmp_output.name)

        try:
            run_wikipedia_awards(limit=1, dataset_path=dataset_path, output_path=output_path)
            new_awards = load_json_list(output_path)
            if not new_awards:
                return
            for row in new_awards:
                row.setdefault("track_key", job.track_key)
            merged = self._merge_by_track_key(load_json_list(WIKI_AWARDS_JSON), new_awards)
            atomic_write_json(WIKI_AWARDS_JSON, merged)
        finally:
            dataset_path.unlink(missing_ok=True)
            output_path.unlink(missing_ok=True)


def run_pipelined_stages(pipeline: SongPipeline, jobs: List[SongJob]) -> None:
    """Push songs through the four stages with stage-level parallelism."""
    stage_audio = queue.Queue()
    stage_links = queue.Queue()
    stage_comments = queue.Queue()
    stage_awards = queue.Queue()

    progress = tqdm(total=len(jobs), desc="Processing songs", unit="song")

    def worker(
        in_q: queue.Queue,
        out_q: Optional[queue.Queue],
        handler,
        label: str,
        finalize: bool = False,
    ):
        while True:
            job = in_q.get()
            if job is None:
                if out_q:
                    out_q.put(None)
                in_q.task_done()
                break
            try:
                handler(job)
            except Exception as exc:  # pragma: no cover - defensive logging
                print(f"[{label}] Error processing {job.label()}: {exc}")
            if finalize:
                progress.update(1)
            if out_q:
                out_q.put(job)
            in_q.task_done()

    threads = [
        threading.Thread(
            target=worker,
            args=(stage_audio, stage_links, pipeline.process_audio_metadata, "audio"),
            daemon=True,
        ),
        threading.Thread(
            target=worker,
            args=(stage_links, stage_comments, pipeline.process_youtube_links, "youtube-link"),
            daemon=True,
        ),
        threading.Thread(
            target=worker,
            args=(stage_comments, stage_awards, pipeline.process_youtube_comments, "comments"),
            daemon=True,
        ),
        threading.Thread(
            target=worker,
            args=(stage_awards, None, pipeline.process_wikipedia_awards, "awards", True),
            daemon=True,
        ),
    ]

    for thread in threads:
        thread.start()

    for job in jobs:
        stage_audio.put(job)
    stage_audio.put(None)

    stage_audio.join()
    stage_links.join()
    stage_comments.join()
    stage_awards.join()

    progress.close()
    for thread in threads:
        thread.join()


def assemble_records(comment_limit: int) -> List[Dict]:
    base_records = assembler.load_records(SONGS_DB_JSON)
    audio_metadata_records = assembler.load_optional_records(AUDIO_METADATA_JSON)
    spotify_feature_records = assembler.load_optional_records(SPOTIFY_FEATURES_CSV)
    youtube_link_records = assembler.load_optional_records(YOUTUBE_LINKS_JSON)
    comment_records = assembler.load_optional_records(YOUTUBE_COMMENTS_JSON)
    award_records = assembler.load_optional_records(WIKI_AWARDS_JSON)

    assembler.merge_audio_metadata(base_records, assembler.build_audio_metadata_map(audio_metadata_records))
    assembler.merge_spotify_features(base_records, assembler.build_spotify_feature_map(spotify_feature_records))
    assembler.merge_youtube_links(base_records, assembler.build_youtube_link_map(youtube_link_records))
    assembler.merge_comments(base_records, assembler.build_comment_map(comment_records), limit=comment_limit)
    assembler.merge_awards(base_records, assembler.build_award_map(award_records))

    cleaned = [assembler.clean_record(row) for row in base_records]
    return cleaned


def main() -> None:
    args = parse_args()
    comment_limit = clamp_comment_limit(args.comments)

    ensure_song_database()

    existing_yaml = load_yaml(FINAL_YAML)
    existing_keys = {make_key(rec) for rec in existing_yaml}
    current_count = len(existing_yaml)
    target_total = current_count + args.n
    print(f"Existing YAML entries: {current_count}. Target after run: {target_total}.")

    base_records = assembler.load_records(SONGS_DB_JSON)
    if current_count >= len(base_records):
        print("All songs from the base dataset are already in the YAML.")
        return

    order_map = build_order_map(base_records)
    jobs: List[SongJob] = []
    for idx in range(current_count, min(target_total, len(base_records))):
        rec = base_records[idx]
        jobs.append(SongJob(index=idx, record=rec, track_key=make_key(rec)))

    if jobs:
        pipeline = SongPipeline(
            base_records=base_records,
            order_map=order_map,
            comment_limit=comment_limit,
            sample_rate=args.sample_rate,
            duration=args.duration,
        )
        run_pipelined_stages(pipeline, jobs)
    else:
        print("No new songs requested; skipping processing.")

    merged_records = assemble_records(comment_limit=comment_limit)

    output: List[Dict] = []
    seen_keys = set()
    partials: List[str] = []
    for rec in merged_records:
        key = make_key(rec)
        if key in seen_keys:
            continue
        if key in existing_keys:
            output.append(rec)
            seen_keys.add(key)
        elif len(output) < target_total:
            missing = missing_fields(rec)
            if missing:
                partials.append(f"{rec.get('name')} - {rec.get('artist')}: missing {', '.join(missing)}")
            output.append(rec)
            seen_keys.add(key)
        if len(output) >= target_total:
            break

    if partials:
        print("Added songs with missing fields:")
        for line in partials:
            print(f"  - {line}")

    FINAL_YAML.parent.mkdir(parents=True, exist_ok=True)
    with FINAL_YAML.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(output, handle, sort_keys=False, allow_unicode=True)
    print(f"Wrote {len(output)} entries to {FINAL_YAML}")


if __name__ == "__main__":
    main()
