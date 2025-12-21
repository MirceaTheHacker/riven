"""Plex Watchlist Module"""

from typing import Generator

import httpx
from kink import di
from loguru import logger
from requests import HTTPError

from program.apis.plex_api import PlexAPI
from program.db.db_functions import item_exists_by_any_id
from program.media.item import MediaItem
from program.settings.manager import settings_manager


class PlexWatchlist:
    """Class for managing Plex Watchlists"""

    def __init__(self):
        self.key = "plex_watchlist"
        self.settings = settings_manager.settings.content.plex_watchlist
        self.w2p_settings = settings_manager.settings.content.watchlist2plex
        self.api = None
        self.initialized = self.validate()
        if not self.initialized:
            return
        logger.success("Plex Watchlist initialized!")

    def validate(self):
        if not self.settings.enabled:
            return False
        if not settings_manager.settings.updaters.plex.token:
            logger.error("Plex token is not set!")
            return False
        try:
            self.api = di[PlexAPI]
            self.api.validate_account()
        except Exception as e:
            logger.error(f"Unable to authenticate Plex account: {e}")
            return False
        if self.settings.rss:
            self.api.set_rss_urls(self.settings.rss)
            for rss_url in self.settings.rss:
                try:
                    response = self.api.validate_rss(rss_url)
                    response.raise_for_status()
                    self.api.rss_enabled = True
                except HTTPError as e:
                    if e.response.status_code == 404:
                        logger.warning(
                            f"Plex RSS URL {rss_url} is Not Found. Please check your RSS URL in settings."
                        )
                        return False
                    else:
                        logger.warning(
                            f"Plex RSS URL {rss_url} is not reachable (HTTP status code: {e.response.status_code})."
                        )
                        return False
                except Exception as e:
                    logger.error(
                        f"Failed to validate Plex RSS URL {rss_url}: {e}", exc_info=True
                    )
                    return False
        return True

    def _build_w2p_payload(self, watchlist_items: list[dict[str, str]]) -> list[dict]:
        payload = []
        for d in watchlist_items:
            title = d.get("title")
            item_type = d.get("type") or "movie"
            identifier = d.get("imdb_id") or d.get("tmdb_id") or d.get("tvdb_id") or title
            if not title or not identifier:
                continue
            payload.append(
                {
                    "id": identifier,
                    "title": title,
                    "year": d.get("year"),
                    "type": "movie" if item_type == "movie" else "show",
                    "season": None,
                    "episode": None,
                }
            )
        return payload

    def _call_w2p(self, items_payload: list[dict]) -> dict[str, dict]:
        if not self.w2p_settings.enabled:
            return {}
        if not items_payload:
            return {}

        headers = {}
        if self.w2p_settings.auth_header_name and self.w2p_settings.auth_header_value:
            headers[self.w2p_settings.auth_header_name] = self.w2p_settings.auth_header_value

        # Use /riven/harvest-item endpoint which accepts items in JSON body
        # The default URL might be /riven/watchlist, so we need to construct the correct endpoint
        base_url = self.w2p_settings.url.rstrip("/")
        if base_url.endswith("/watchlist"):
            harvest_url = base_url.replace("/watchlist", "/harvest-item")
        elif not base_url.endswith("/harvest-item"):
            harvest_url = f"{base_url}/harvest-item"
        else:
            harvest_url = base_url

        try:
            with httpx.Client(timeout=120.0) as client:  # Increased timeout for browser automation
                resp = client.post(
                    harvest_url,
                    json={"items": items_payload},
                    headers=headers,
                )
                resp.raise_for_status()
                data = resp.json()
                logger.debug(f"W2P harvest returned {len(data.get('items', []))} items")
        except Exception as e:
            logger.error(f"Failed calling Watchlist2Plex harvest endpoint {harvest_url}: {e}")
            return {}

        releases_map: dict[str, dict] = {}
        for entry in data.get("items", []):
            item = entry.get("item", {})
            ident = item.get("id") or item.get("title")
            if not ident:
                continue
            releases_map[str(ident)] = entry
        return releases_map

    def run(self) -> Generator[MediaItem, None, None]:
        """Fetch new media from `Plex Watchlist` and RSS feed if enabled."""
        try:
            watchlist_items: list[dict[str, str]] = self.api.get_items_from_watchlist()
            rss_items: list[tuple[str, str]] = (
                self.api.get_items_from_rss() if self.api.rss_enabled else []
            )
        except Exception as e:
            logger.warning(f"Error fetching items: {e}")
            return

        items_to_yield: list[MediaItem] = []

        # Harvest releases via W2P for watchlist items
        if watchlist_items:
            logger.info(f"Calling W2P to harvest {len(watchlist_items)} watchlist items")
            w2p_payload = self._build_w2p_payload(watchlist_items)
            logger.debug(f"W2P payload built: {len(w2p_payload)} items")
            w2p_results = self._call_w2p(w2p_payload)
            logger.info(f"W2P returned {len(w2p_results)} results")

            # Build a mapping of identifier -> watchlist item for easier lookup
            ident_to_watchlist_item = {}
            for d in watchlist_items:
                # Use the same identifier logic as _build_w2p_payload
                identifier = d.get("imdb_id") or d.get("tmdb_id") or d.get("tvdb_id") or d.get("title")
                if identifier:
                    ident_to_watchlist_item[str(identifier)] = d

            # Process W2P results and match them back to watchlist items
            matched_count = 0
            for w2p_entry in w2p_results.values():
                w2p_item = w2p_entry.get("item", {})
                w2p_id = w2p_item.get("id") or w2p_item.get("title")
                releases = w2p_entry.get("releases") or []
                
                if not releases:
                    logger.debug(f"Skipping {w2p_item.get('title')} - no W2P releases")
                    continue
                
                # Find the matching watchlist item
                d = ident_to_watchlist_item.get(str(w2p_id)) if w2p_id else None
                if not d:
                    logger.warning(f"Could not match W2P result {w2p_id} ({w2p_item.get('title')}) to watchlist item. Available IDs: {list(ident_to_watchlist_item.keys())[:5]}")
                    continue

                # Build item data using the watchlist item's IDs
                if d.get("tvdb_id") and not d.get("tmdb_id"):
                    item_data = {"tvdb_id": d["tvdb_id"], "requested_by": self.key}
                elif d.get("tmdb_id"):
                    item_data = {"tmdb_id": d["tmdb_id"], "requested_by": self.key}
                else:
                    # fallback to imdb-only
                    item_data = {"imdb_id": d.get("imdb_id"), "requested_by": self.key}

                item_data["aliases"] = {"w2p_releases": releases}
                items_to_yield.append(MediaItem(item_data))
                matched_count += 1
                logger.info(f"Matched {d.get('title')} with {len(releases)} releases from W2P")
            
            if matched_count == 0 and w2p_results:
                logger.warning(f"W2P returned {len(w2p_results)} results but none matched watchlist items. This may indicate an ID mismatch issue.")

        if rss_items:
            for r in rss_items:
                _type, _id = r
                if _type == "show":
                    items_to_yield.append(
                        MediaItem({"tvdb_id": _id, "requested_by": self.key})
                    )
                elif _type == "movie":
                    items_to_yield.append(
                        MediaItem({"tmdb_id": _id, "requested_by": self.key})
                    )

        if items_to_yield:
            items_to_yield = [
                item
                for item in items_to_yield
                if not item_exists_by_any_id(
                    imdb_id=item.imdb_id, tvdb_id=item.tvdb_id, tmdb_id=item.tmdb_id
                )
            ]

        logger.info(f"Fetched {len(items_to_yield)} new items from plex watchlist")
        yield items_to_yield
