import json
from pathlib import Path
from typing import Dict, List, Optional

import scrapy

from ..items import WikipediaAwardItem


class WikipediaAwardsSpider(scrapy.Spider):
    name = "wikipedia_awards"
    allowed_domains = ["en.wikipedia.org"]

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

        rows = [
            {
                "name": row.get("name"),
                "artist": row.get("artist"),
                "year": row.get("year"),
                "link": row.get("link"),
            }
            for row in data
            if row.get("link")
        ]
        return rows if self.limit is None else rows[: self.limit]

    def start_requests(self):
        for entry in self.entries:
            yield scrapy.Request(
                url=entry["link"],
                callback=self.parse_awards,
                cb_kwargs={"entry": entry},
                headers={"Accept-Language": "en-US,en;q=0.9"},
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
        keywords = ("award", "accolade", "honor", "honour")
        for heading in response.xpath("//h2[span[@class='mw-headline']]"):
            title = " ".join(heading.xpath(".//span[@class='mw-headline']//text()").getall()).strip().lower()
            if not any(key in title for key in keywords):
                continue

            for node in heading.xpath("./following-sibling::*"):
                tag = node.root.tag if hasattr(node, "root") else node.xpath("name()").get()
                if tag == "h2":
                    break
                if tag in {"ul", "ol"}:
                    for bullet in node.xpath(".//li"):
                        text = " ".join(bullet.css("::text").getall()).strip()
                        if text:
                            awards.append(text)
                elif tag == "table":
                    for row in node.xpath(".//tr[th or td]"):
                        text = " ".join(row.css("::text").getall()).strip()
                        if text:
                            awards.append(text)
        return awards
