from typing import Dict

from loguru import logger

from program.media.item import MediaItem
from program.services.scrapers.shared import _parse_results
from program.settings.manager import settings_manager


class Watchlist2PlexScraper:
    key = "watchlist2plex"

    def __init__(self):
        # Always enable this scraper - it only processes items that have W2P releases
        # If an item doesn't have W2P releases, the scraper will return an empty dict
        # This allows W2P releases to be used even if settings model doesn't have watchlist2plex
        try:
            w2p_settings = getattr(settings_manager.settings.content, "watchlist2plex", None)
            if w2p_settings:
                self.initialized = getattr(w2p_settings, "enabled", True)  # Default to True if enabled attr missing
            else:
                # If settings not found, enable anyway - scraper is safe to run
                self.initialized = True
                logger.debug("Watchlist2PlexScraper: watchlist2plex settings not found in ContentModel, enabling scraper anyway")
        except Exception as e:
            logger.warning(f"Watchlist2PlexScraper: Failed to check settings, enabling anyway: {e}")
            self.initialized = True  # Enable on error - scraper is safe to run
        
        if self.initialized:
            logger.info("Watchlist2PlexScraper initialized and enabled")
        else:
            logger.warning("Watchlist2PlexScraper initialized but disabled")

    def run(self, item: MediaItem) -> Dict[str, str]:
        """Return mapping of infohash -> raw title using W2P releases stored on the item."""
        releases = {}
        aliases = getattr(item, "aliases", {}) or {}
        w2p_releases = aliases.get("w2p_releases") or []
        
        logger.info(f"Watchlist2PlexScraper.run() called for {item.log_string}, found {len(w2p_releases)} W2P releases in aliases")
        
        if not w2p_releases:
            logger.debug(f"No W2P releases found in aliases for {item.log_string}. Aliases keys: {list(aliases.keys())}")
            return releases

        for rel in w2p_releases:
            infohash = rel.get("infohash")
            if not infohash and rel.get("magnet"):
                infohash = self._extract_infohash(rel["magnet"])
            if not infohash:
                logger.debug(f"W2P release missing infohash: {rel.get('title', 'unknown')}")
                continue
            
            # Extract and clean the title - W2P titles may contain newlines, emojis, and extra formatting
            raw_title = rel.get("title") or rel.get("raw_title") or ""
            if isinstance(raw_title, str):
                # Remove newlines, emojis, and extra whitespace
                import re
                # Remove emojis and special unicode characters (keep basic ASCII and common punctuation)
                cleaned_title = re.sub(r'[^\x00-\x7F]+', ' ', raw_title)
                # Remove newlines and extra whitespace
                cleaned_title = re.sub(r'\s+', ' ', cleaned_title).strip()
                # Take only the first line if there are multiple lines (before newline)
                cleaned_title = cleaned_title.split('\n')[0].strip()
                releases[infohash.lower()] = cleaned_title
            elif isinstance(raw_title, dict):
                # If title is a dict, try to extract a string value
                logger.warning(f"W2P release has dict title instead of string: {raw_title}, using infohash as fallback")
                releases[infohash.lower()] = f"W2P Release {infohash[:8]}"
            else:
                # Fallback to empty string
                releases[infohash.lower()] = str(raw_title) if raw_title else ""

        if not releases:
            logger.warning(f"No W2P infohashes extracted for {item.log_string} despite {len(w2p_releases)} releases")
        else:
            logger.info(f"Watchlist2PlexScraper extracted {len(releases)} infohashes from W2P releases for {item.log_string}")
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
