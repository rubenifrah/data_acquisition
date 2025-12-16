import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import scrapy

from ..items import YoutubeCommentItem


class YoutubeCommentsSpider(scrapy.Spider):
    name = "youtube_comments"
    allowed_domains = ["www.youtube.com", "youtube.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "CONCURRENT_REQUESTS": 2,
    }

    def __init__(self, links_path: str = "data/youtube_links.json", limit: Optional[int] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.links_path = Path(links_path)
        self.limit = int(limit) if limit else None
        self.video_entries = self._load_video_entries()

    def _load_video_entries(self) -> List[Dict[str, str]]:
        if not self.links_path.exists():
            self.logger.warning("YouTube links file not found: %s", self.links_path)
            return []

        with self.links_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        entries: List[Dict[str, str]] = []
        for row in data:
            video_id = row.get("youtube_id") or self._extract_id_from_url(row.get("youtube_url", ""))
            if not video_id:
                continue
            entries.append(
                {
                    "youtube_id": video_id,
                    "name": row.get("name"),
                    "artist": row.get("artist"),
                    "year": row.get("year"),
                }
            )
        return entries if self.limit is None else entries[: self.limit]

    @staticmethod
    def _extract_id_from_url(url: str) -> Optional[str]:
        match = re.search(r"v=([A-Za-z0-9_-]{6,})", url)
        return match.group(1) if match else None

    def start_requests(self):
        for entry in self.video_entries:
            video_id = entry["youtube_id"]
            url = f"https://www.youtube.com/watch?v={video_id}&pbj=1"
            yield scrapy.Request(
                url=url,
                callback=self.parse_watch_page,
                cb_kwargs={"entry": entry},
                headers={"Accept-Language": "en-US,en;q=0.9"},
            )

    def parse_watch_page(self, response: scrapy.http.Response, entry: Dict[str, str]):
        initial_data = self._extract_initial_data(response.text)
        api_key = self._extract_api_key(response.text)
        continuation = self._find_first_continuation(initial_data)

        if not api_key or not continuation:
            self.logger.warning("Missing API key or continuation for %s", entry)
            return

        yield self._build_comment_request(
            api_key=api_key,
            continuation=continuation,
            entry=entry,
            rank_offset=0,
        )

    def _build_comment_request(
        self,
        api_key: str,
        continuation: str,
        entry: Dict[str, str],
        rank_offset: int,
    ) -> scrapy.Request:
        body = {
            "context": {
                "client": {
                    "hl": "en",
                    "gl": "US",
                    "clientName": "WEB",
                    "clientVersion": "2.20241008.00.00",
                }
            },
            "continuation": continuation,
        }
        return scrapy.Request(
            url=f"https://www.youtube.com/youtubei/v1/next?key={api_key}",
            method="POST",
            body=json.dumps(body),
            headers={"Content-Type": "application/json"},
            callback=self.parse_comments,
            cb_kwargs={
                "entry": entry,
                "rank_offset": rank_offset,
                "api_key": api_key,
            },
        )

    def parse_comments(
        self,
        response: scrapy.http.Response,
        entry: Dict[str, str],
        rank_offset: int,
        api_key: str,
    ):
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse comments payload for %s", entry)
            return

        position = rank_offset
        for renderer in self._iter_comment_renderers(payload):
            position += 1
            if position > 10:
                break

            text_runs = renderer.get("contentText", {}).get("runs", [])
            comment_text = "".join(run.get("text", "") for run in text_runs)

            yield YoutubeCommentItem(
                track_name=entry.get("name"),
                artist=entry.get("artist"),
                youtube_id=entry.get("youtube_id"),
                comment_id=renderer.get("commentId"),
                author=renderer.get("authorText", {}).get("simpleText"),
                text=comment_text,
                like_count=renderer.get("likeCount"),
                published_at=renderer.get("publishedTimeText", {}).get("simpleText"),
                position=position,
            )

        if position < 10:
            continuation = self._find_first_continuation(payload)
            if continuation:
                yield self._build_comment_request(
                    api_key=api_key,
                    continuation=continuation,
                    entry=entry,
                    rank_offset=position,
                )

    @staticmethod
    def _extract_initial_data(html: str) -> Dict:
        match = re.search(r"ytInitialData\"?\s*:\s*({.*?})\s*[,;]</script>", html, re.DOTALL)
        if not match:
            return {}
        try:
            return json.loads(match.group(1))
        except Exception:
            return {}

    @staticmethod
    def _extract_api_key(html: str) -> Optional[str]:
        match = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', html)
        return match.group(1) if match else None

    def _find_first_continuation(self, obj) -> Optional[str]:
        if isinstance(obj, dict):
            if "continuationEndpoint" in obj:
                token = obj["continuationEndpoint"].get("continuationCommand", {}).get("token")
                if token:
                    return token
            if "nextContinuationData" in obj:
                token = obj["nextContinuationData"].get("continuation")
                if token:
                    return token
            for value in obj.values():
                token = self._find_first_continuation(value)
                if token:
                    return token
        elif isinstance(obj, list):
            for item in obj:
                token = self._find_first_continuation(item)
                if token:
                    return token
        return None

    def _iter_comment_renderers(self, payload: Dict) -> Iterable[Dict]:
        actions = payload.get("onResponseReceivedEndpoints", [])
        for action in actions:
            containers = (
                action.get("reloadContinuationItemsCommand", {}).get("continuationItems")
                or action.get("appendContinuationItemsAction", {}).get("continuationItems")
                or []
            )
            for container in containers:
                renderer = (
                    container.get("commentThreadRenderer", {})
                    .get("comment", {})
                    .get("commentRenderer")
                ) or container.get("commentRenderer")
                if renderer:
                    yield renderer
