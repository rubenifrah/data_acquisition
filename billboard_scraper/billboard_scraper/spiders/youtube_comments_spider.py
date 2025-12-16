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
        "TELNETCONSOLE_ENABLED": False,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(
        self,
        links_path: str = "data/youtube_links.json",
        limit: Optional[int] = None,
        max_comments: int = 10,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.links_path = Path(links_path)
        self.limit = int(limit) if limit else None
        self.max_comments = int(max_comments)
        self.video_entries = self._load_video_entries()

    def _load_video_entries(self) -> List[Dict[str, str]]:
        if not self.links_path.exists():
            self.logger.warning("YouTube links file not found: %s", self.links_path)
            return []

        with self.links_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        entries: List[Dict[str, str]] = []
        for row in data:
            candidates_raw = row.get("youtube_candidates") or []
            candidates: List[Dict[str, str]] = []

            # Parse explicit candidates list if present.
            for cand in candidates_raw:
                vid = cand.get("youtube_id") or self._extract_id_from_url(cand.get("youtube_url", ""))
                if not vid:
                    continue
                candidates.append(
                    {
                        "youtube_id": vid,
                        "youtube_url": cand.get("youtube_url") or f"https://www.youtube.com/watch?v={vid}",
                    }
                )

            # Backward compatibility for single youtube_id/url fields.
            if not candidates:
                video_id = row.get("youtube_id") or self._extract_id_from_url(row.get("youtube_url", ""))
                if video_id:
                    candidates.append(
                        {
                            "youtube_id": video_id,
                            "youtube_url": row.get("youtube_url") or f"https://www.youtube.com/watch?v={video_id}",
                        }
                    )

            if not candidates:
                continue

            entries.append(
                {
                    "youtube_id": candidates[0]["youtube_id"],
                    "candidates": candidates,
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
            yield from self._make_watch_request(entry, candidate_index=0)

    def _make_watch_request(self, entry: Dict[str, str], candidate_index: int):
        candidates = entry.get("candidates") or []
        if candidate_index >= len(candidates):
            return []
        video_id = candidates[candidate_index]["youtube_id"]
        url = f"https://www.youtube.com/watch?v={video_id}"
        return [
            scrapy.Request(
                url=url,
                callback=self.parse_watch_page,
                cb_kwargs={"entry": entry, "candidate_index": candidate_index},
                headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
                    ),
                },
            )
        ]

    def parse_watch_page(self, response: scrapy.http.Response, entry: Dict[str, str], candidate_index: int):
        initial_data = self._extract_initial_data(response.text)
        api_key = self._extract_api_key(response.text)
        continuation = self._extract_comment_continuation(initial_data) or self._find_first_continuation(initial_data)
        visitor_data = self._extract_visitor_data(response.text)

        if not api_key or not continuation:
            self.logger.warning("Missing API key or continuation for %s (candidate %s)", entry, candidate_index)
            yield from self._fallback_to_next_candidate(entry, candidate_index)
            return

        yield self._build_comment_request(
            api_key=api_key,
            continuation=continuation,
            entry=entry,
            rank_offset=0,
            comments_collected=0,
            candidate_index=candidate_index,
            visitor_data=visitor_data,
        )

    def _build_comment_request(
        self,
        api_key: str,
        continuation: str,
        entry: Dict[str, str],
        rank_offset: int,
        visitor_data: Optional[str],
        comments_collected: int,
        candidate_index: int,
    ) -> scrapy.Request:
        client_context = {
            "hl": "en",
            "gl": "US",
            "clientName": "WEB",
            "clientVersion": "2.20241008.00.00",
        }
        if visitor_data:
            client_context["visitorData"] = visitor_data

        body = {
            "context": {
                "client": client_context
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
                "visitor_data": visitor_data,
                "comments_collected": comments_collected,
                "candidate_index": candidate_index,
            },
        )

    def parse_comments(
        self,
        response: scrapy.http.Response,
        entry: Dict[str, str],
        rank_offset: int,
        api_key: str,
        visitor_data: Optional[str],
        comments_collected: int,
        candidate_index: int,
    ):
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse comments payload for %s", entry)
            yield from self._fallback_to_next_candidate(entry, candidate_index)
            return

        candidates = entry.get("candidates") or []
        youtube_id = None
        if 0 <= candidate_index < len(candidates):
            youtube_id = candidates[candidate_index].get("youtube_id")

        position = rank_offset
        for renderer in self._extract_comment_entities(payload):
            position += 1
            if position > self.max_comments:
                break

            comment_text = renderer.get("text") or ""

            yield YoutubeCommentItem(
                track_name=entry.get("name"),
                artist=entry.get("artist"),
                youtube_id=youtube_id or entry.get("youtube_id"),
                comment_id=renderer.get("commentId"),
                author=renderer.get("authorText", {}).get("simpleText"),
                text=comment_text,
                like_count=renderer.get("likeCount"),
                published_at=renderer.get("publishedTimeText", {}).get("simpleText") or renderer.get("publishedTime"),
                position=position,
            )

        total_comments = comments_collected + max(0, position - rank_offset)

        if position < self.max_comments:
            continuation = self._find_first_continuation(payload)
            if continuation:
                yield self._build_comment_request(
                    api_key=api_key,
                    continuation=continuation,
                    entry=entry,
                    rank_offset=position,
                    comments_collected=total_comments,
                    candidate_index=candidate_index,
                    visitor_data=visitor_data,
                )
                return

        if total_comments == 0:
            yield from self._fallback_to_next_candidate(entry, candidate_index)

    def _extract_comment_entities(self, payload: Dict) -> Iterable[Dict]:
        seen: set[str] = set()

        # Modern commentEntityPayload path
        mutations = (
            payload.get("frameworkUpdates", {})
            .get("entityBatchUpdate", {})
            .get("mutations", [])
        )
        for mutation in mutations:
            entity = mutation.get("payload", {}).get("commentEntityPayload", {})
            props = entity.get("properties") or {}
            if not props:
                continue
            comment_id = props.get("commentId")
            if comment_id and comment_id in seen:
                continue
            text = None
            content = props.get("content")
            if isinstance(content, dict):
                text = content.get("content")
            elif isinstance(content, str):
                text = content

            result = {
                "commentId": comment_id,
                "text": text,
                "likeCount": props.get("likeCount"),
                "publishedTime": props.get("publishedTime"),
                "authorText": {"simpleText": (entity.get("author") or {}).get("displayName") or props.get("authorButtonA11y")},
            }
            if comment_id:
                seen.add(comment_id)
            yield result

        # Legacy commentRenderer path (some responses still use it)
        for renderer in self._iter_comment_renderers(payload):
            comment_id = renderer.get("commentId")
            if comment_id and comment_id in seen:
                continue
            text_runs = renderer.get("contentText", {}).get("runs", [])
            comment_text = "".join(run.get("text", "") for run in text_runs)
            renderer = dict(renderer)
            renderer["text"] = comment_text
            if comment_id:
                seen.add(comment_id)
            yield renderer

    @staticmethod
    def _extract_initial_data(html: str) -> Dict:
        patterns = [
            r"ytInitialData\"?\s*[:=]\s*({.*?})\s*;</script",
            r"window\[\s*\"ytInitialData\"\s*\]\s*=\s*({.*?});",
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if not match:
                continue
            try:
                return json.loads(match.group(1))
            except Exception:
                continue
        try:
            return json.loads(html)
        except Exception:
            return {}

    @staticmethod
    def _extract_api_key(html: str) -> Optional[str]:
        for pattern in (r'"INNERTUBE_API_KEY":"([^"]+)"', r'"innertubeApiKey":"([^"]+)"'):
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _extract_visitor_data(html: str) -> Optional[str]:
        match = re.search(r'"VISITOR_DATA":"([^"]+)"', html)
        return match.group(1) if match else None

    def _extract_comment_continuation(self, data: Dict) -> Optional[str]:
        try:
            contents = (
                data.get("contents", {})
                .get("twoColumnWatchNextResults", {})
                .get("results", {})
                .get("results", {})
                .get("contents", [])
            )
        except AttributeError:
            return None

        for content in contents:
            section = (
                content.get("itemSectionRenderer", {})
                .get("contents", [{}])[0]
                .get("commentSectionRenderer")
            )
            if not section:
                continue
            continuations = section.get("continuations") or []
            for cont in continuations:
                token = cont.get("nextContinuationData", {}).get("continuation")
                if token:
                    return token
        return None

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

    def _fallback_to_next_candidate(self, entry: Dict[str, str], candidate_index: int):
        candidates = entry.get("candidates") or []
        next_index = candidate_index + 1
        if next_index >= len(candidates) or next_index >= 4:
            self.logger.info(
                "No comments found after trying %s candidates for %s", next_index, entry.get("name")
            )
            return []
        self.logger.info(
            "Retrying with candidate %s for %s (previous had no comments)", next_index + 1, entry.get("name")
        )
        return self._make_watch_request(entry, candidate_index=next_index)

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
