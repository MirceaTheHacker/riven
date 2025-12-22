from datetime import datetime
from typing import Generator, List

import httpx
from loguru import logger

from program.media.item import MediaItem, Movie, Show
from program.settings.manager import settings_manager
from program.db import db_functions


class Watchlist2PlexContent:
    """Ingest watchlist items and release data from Watchlist2Plex (harvest mode)."""

    def __init__(self):
        self.key = "watchlist2plex"
        self.settings = settings_manager.settings.content.watchlist2plex
        self.initialized = self.validate()

    def validate(self) -> bool:
        # Disable this service - PlexWatchlist handles W2P integration now
        # This prevents duplicate W2P calls and conflicts
        logger.debug("Watchlist2PlexContent service is disabled - PlexWatchlist handles W2P integration")
        return False
        # Original validation code (kept for reference):
        # if not self.settings.enabled:
        #     return False
        # if not self.settings.url:
        #     logger.error("Watchlist2Plex URL is not set.")
        #     return False
        # return True

    def _headers(self) -> dict:
        headers = {}
        if self.settings.auth_header_name and self.settings.auth_header_value:
            headers[self.settings.auth_header_name] = self.settings.auth_header_value
        return headers

    def _make_request(self) -> dict:
        params = {
            "force": str(self.settings.force).lower(),
            "limit": self.settings.limit,
        }
        # W2P can take a while to harvest items (browser automation + DMM),
        # so allow a generous timeout.
        with httpx.Client(timeout=120.0) as client:
            resp = client.post(self.settings.url, params=params, headers=self._headers())
            resp.raise_for_status()
            return resp.json()

    def _build_item(self, payload: dict, releases: List[dict]) -> MediaItem:
        base = payload.get("item", payload)
        title = base.get("title")
        year = base.get("year")
        item_type = base.get("type", "movie")
        synthetic_id = base.get("id") or f"{title}:{year or 'na'}:{item_type}"
        aliases = {"w2p_releases": releases}

        data = {
            "title": title,
            "year": year,
            "requested_by": self.key,
            "requested_at": datetime.now(),
            "aliases": aliases,
            "imdb_id": f"w2p:{synthetic_id}",
        }

        if item_type == "movie":
            return Movie(data)
        return Show(data)

    def run(self) -> Generator[List[MediaItem], None, None]:
        # Disabled - PlexWatchlist handles W2P integration now
        # This prevents duplicate W2P calls and conflicts
        logger.debug("Watchlist2PlexContent.run() called but service is disabled - returning early")
        return
        
        if not self.initialized:
            return

        try:
            payload = self._make_request()
        except Exception as e:
            logger.error(f"W2P request failed: {e}")
            return

        items_payload = payload.get("items") or []
        new_items: List[MediaItem] = []
        for entry in items_payload:
            releases = entry.get("releases") or []
            if not releases:
                logger.debug("Skipping W2P item with no releases: %s", entry)
                continue
            item_obj = self._build_item(entry, releases)
            # Deduplicate by ids to avoid re-adding
            if db_functions.item_exists_by_any_id(
                None,
                None,
                None,
                item_obj.imdb_id,
            ):
                logger.debug(
                    f"W2P item already exists (id={item_obj.imdb_id}), skipping"
                )
                continue
            new_items.append(item_obj)

        if new_items:
            logger.info(f"W2P harvested {len(new_items)} items")
            yield new_items
        else:
            logger.info("W2P returned no new items")
