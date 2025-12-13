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

        params = {
            "force": str(self.w2p_settings.force).lower(),
            "limit": self.w2p_settings.limit,
        }

        try:
            with httpx.Client(timeout=60.0) as client:
                resp = client.post(
                    self.w2p_settings.url,
                    json={"items": items_payload},
                    headers=headers,
                    params=params,
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.error(f"Failed calling Watchlist2Plex harvest endpoint: {e}")
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
        w2p_payload = self._build_w2p_payload(watchlist_items)
        w2p_results = self._call_w2p(w2p_payload)

        if watchlist_items:
            for d in watchlist_items:
                ident = d.get("imdb_id") or d.get("tmdb_id") or d.get("tvdb_id")
                w2p_entry = w2p_results.get(str(ident)) if ident else None
                releases = (w2p_entry or {}).get("releases") or []
                if not releases:
                    logger.debug(f"Skipping {d.get('title')} - no W2P releases")
                    continue

                if d.get("tvdb_id") and not d.get("tmdb_id"):
                    item_data = {"tvdb_id": d["tvdb_id"], "requested_by": self.key}
                elif d.get("tmdb_id"):
                    item_data = {"tmdb_id": d["tmdb_id"], "requested_by": self.key}
                else:
                    # fallback to imdb-only
                    item_data = {"imdb_id": d.get("imdb_id"), "requested_by": self.key}

                item_data["aliases"] = {"w2p_releases": releases}
                items_to_yield.append(MediaItem(item_data))

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
