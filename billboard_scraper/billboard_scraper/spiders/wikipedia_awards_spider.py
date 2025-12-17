import json
from pathlib import Path
from typing import Dict, List, Optional

import scrapy

from ..items import WikipediaAwardItem


class WikipediaAwardsSpider(scrapy.Spider):
    name = "wikipedia_awards"
    allowed_domains = ["en.wikipedia.org"]

    custom_settings = {
        "TELNETCONSOLE_ENABLED": False,
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    def __init__(self, dataset_path: str = "data/songs_database.json", limit: Optional[int] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_path = Path(dataset_path)
        self.limit = int(limit) if limit else None
        self.entries = self._load_entries()

    def _load_entries(self) -> List[Dict[str, str]]:
        if not self.dataset_path.exists():
            self.logger.warning("Dataset not found at %s", self.dataset_path)
            return []

        with self.dataset_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        def pick_link(row: Dict) -> Optional[str]:
            return row.get("link") or row.get("wikipedia_link")

        rows = [
            {
                "name": row.get("name"),
                "artist": row.get("artist"),
                "year": row.get("year"),
                "link": pick_link(row),
            }
            for row in data
            if pick_link(row)
        ]
        return rows if self.limit is None else rows[: self.limit]

    def start_requests(self):
        for entry in self.entries:
            yield scrapy.Request(
                url=entry["link"],
                callback=self.parse_awards,
                cb_kwargs={"entry": entry},
            )

    def parse_awards(self, response: scrapy.http.Response, entry: Dict[str, str]):
        awards = self._extract_awards(response)
        yield WikipediaAwardItem(
            track_name=entry.get("name"),
            artist=entry.get("artist"),
            year=entry.get("year"),
            source=response.url,
            awards=awards,
        )

    def _extract_awards(self, response: scrapy.http.Response) -> List[str]:
        awards: List[str] = []
        keywords = (
            "award",
            "grammy",
            "accolade",
            "honor",
            "honour",
            "nomination",
            "nominated",
            "won",
            "winning",
            "ranked",
            "ranking",
            "listed",
        )

        def looks_relevant(text: str) -> bool:
            lower = text.lower()
            return any(k in lower for k in keywords)

        root = response.xpath("//div[contains(@class,'mw-parser-output')]")
        seen = set()

        # Award-ish bullets (often live under reception sections)
        for bullet in root.xpath(".//ul/li | .//ol/li"):
            text = " ".join(bullet.css("::text").getall()).strip()
            if text and looks_relevant(text) and text not in seen:
                seen.add(text)
                awards.append(text)

        # Paragraphs often mention wins/nominations inline.
        for para in root.xpath("./p"):
            text = " ".join(para.css("::text").getall()).strip()
            if text and looks_relevant(text) and text not in seen:
                seen.add(text)
                awards.append(text)

        # Capture rows from award/nomination tables
        for row in root.xpath(".//table[contains(@class,'wikitable')]//tr[th or td]"):
            text = " ".join(row.css("::text").getall()).strip()
            if text and looks_relevant(text) and text not in seen:
                seen.add(text)
                awards.append(text)

        return awards
