from typing import Dict

from loguru import logger

from program.media.item import MediaItem
from program.services.scrapers.shared import _parse_results
from program.settings.manager import settings_manager


class Watchlist2PlexScraper:
    key = "watchlist2plex"

    def __init__(self):
        self.initialized = settings_manager.settings.content.watchlist2plex.enabled

    def run(self, item: MediaItem) -> Dict[str, str]:
        """Return mapping of infohash -> raw title using W2P releases stored on the item."""
        releases = {}
        aliases = getattr(item, "aliases", {}) or {}
        w2p_releases = aliases.get("w2p_releases") or []
        if not w2p_releases:
            return releases

        for rel in w2p_releases:
            infohash = rel.get("infohash")
            if not infohash and rel.get("magnet"):
                infohash = self._extract_infohash(rel["magnet"])
            if not infohash:
                continue
            releases[infohash.lower()] = rel.get("title") or rel.get("raw_title") or ""

        if not releases:
            logger.debug(f"No W2P infohashes for {item.log_string}")
        return releases

    @staticmethod
    def _extract_infohash(magnet: str | None) -> str | None:
        import re

        if not magnet:
            return None
        match = re.search(r"btih:([a-fA-F0-9]{32,40})", magnet)
        if match:
            return match.group(1)
        return None

    def scrape_into_streams(self, item: MediaItem) -> Dict[str, str]:
        return self.run(item)

    @staticmethod
    def apply_results(item: MediaItem, results: Dict[str, str]) -> Dict[str, str]:
        """Compatibility helper if needed."""
        return results
