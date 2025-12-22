"""Plex Watchlist Module"""

from typing import Generator

import httpx
from kink import di
from loguru import logger
from requests import HTTPError

from program.apis.plex_api import PlexAPI
from program.db.db_functions import get_item_by_external_id, item_exists_by_any_id
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
                logger.info(f"W2P harvest returned {len(data.get('items', []))} items")
                logger.info(f"W2P harvest response structure: status={data.get('status')}, processed_count={data.get('processed_count')}, items_count={len(data.get('items', []))}")
                # Log first item structure for debugging
                if data.get('items'):
                    first_item = data['items'][0]
                    logger.warning(f"W2P first item structure: keys={list(first_item.keys())}, has_item={('item' in first_item)}, has_releases={('releases' in first_item)}, releases_count={len(first_item.get('releases', []))}")
                    logger.warning(f"W2P first item full structure: {first_item}")
                else:
                    logger.warning(f"W2P returned no items in response. Full response: {data}")
        except Exception as e:
            logger.error(f"Failed calling Watchlist2Plex harvest endpoint {harvest_url}: {e}")
            return {}

        releases_map: dict[str, dict] = {}
        items_list = data.get("items", [])
        logger.info(f"Processing {len(items_list)} items from W2P response")
        
        if not items_list:
            logger.warning(f"W2P returned empty items list. Full response keys: {list(data.keys())}")
            return {}
        
        for idx, entry in enumerate(items_list):
            # Handle both direct structure and nested structure
            if "item" in entry:
                item = entry.get("item", {})
                releases = entry.get("releases", [])
            else:
                # If entry is the item itself (shouldn't happen but handle it)
                item = entry
                releases = entry.get("releases", [])
            
            ident = item.get("id") or item.get("title")
            if not ident:
                logger.warning(f"W2P result #{idx} missing identifier. Entry keys: {list(entry.keys())}, Entry: {entry}")
                continue
            
            logger.warning(f"W2P result for {ident} ({item.get('title', 'unknown')}): {len(releases)} releases. Entry keys: {list(entry.keys())}, Item keys: {list(item.keys())}")
            if releases:
                logger.warning(f"W2P releases sample (first 2): {releases[:2] if len(releases) > 0 else 'N/A'}")
            releases_map[str(ident)] = entry
        
        logger.info(f"Built releases_map with {len(releases_map)} entries")
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
            # Always call W2P for watchlist items to get latest releases
            # This ensures we get the most up-to-date release data from DMM
            items_to_harvest = []
            for item in watchlist_items:
                # Check if item exists in database (for logging purposes)
                existing_item = None
                if item.get("imdb_id"):
                    existing_item = get_item_by_external_id(imdb_id=item["imdb_id"])
                if not existing_item and item.get("tmdb_id"):
                    existing_item = get_item_by_external_id(tmdb_id=item["tmdb_id"])
                if not existing_item and item.get("tvdb_id"):
                    existing_item = get_item_by_external_id(tvdb_id=item["tvdb_id"])
                
                if existing_item:
                    # Check if item already has W2P releases stored (for logging)
                    aliases = getattr(existing_item, "aliases", {}) or {}
                    w2p_releases = aliases.get("w2p_releases") or []
                    if w2p_releases:
                        logger.debug(f"Including {item.get('title', 'unknown')} - exists with {len(w2p_releases)} W2P releases, will refresh from W2P")
                    else:
                        logger.debug(f"Including {item.get('title', 'unknown')} - exists in database but has no W2P releases (will check for better quality)")
                else:
                    logger.debug(f"Including {item.get('title', 'unknown')} - new item, will fetch from W2P")
                
                items_to_harvest.append(item)
            
            if not items_to_harvest:
                logger.info(f"All {len(watchlist_items)} watchlist items already have W2P releases stored, skipping W2P call")
                w2p_results = {}
            else:
                skipped_count = len(watchlist_items) - len(items_to_harvest)
                logger.info(f"Calling W2P to harvest {len(items_to_harvest)} items (skipped {skipped_count} items that already have W2P releases)")
                w2p_payload = self._build_w2p_payload(items_to_harvest)
                logger.info(f"W2P payload built: {len(w2p_payload)} items: {[p.get('title') for p in w2p_payload]}")
                w2p_results = self._call_w2p(w2p_payload)
                logger.info(f"W2P returned {len(w2p_results)} results. Result keys: {list(w2p_results.keys())}")

                # Build a mapping of identifier -> watchlist item for easier lookup
                ident_to_watchlist_item = {}
                for d in items_to_harvest:
                    # Use the same identifier logic as _build_w2p_payload
                    identifier = d.get("imdb_id") or d.get("tmdb_id") or d.get("tvdb_id") or d.get("title")
                    if identifier:
                        ident_to_watchlist_item[str(identifier)] = d

                # Process W2P results and match them back to watchlist items
                matched_count = 0
                skipped_no_releases = 0
                skipped_no_match = 0
                
                for w2p_entry in w2p_results.values():
                    w2p_item = w2p_entry.get("item", {})
                    w2p_id = w2p_item.get("id") or w2p_item.get("title")
                    w2p_title = w2p_item.get("title", "unknown")
                    releases = w2p_entry.get("releases") or []
                    
                    logger.warning(f"Processing W2P result: title={w2p_title}, id={w2p_id}, releases_count={len(releases)}, entry_keys={list(w2p_entry.keys())}, item_keys={list(w2p_item.keys())}")
                    
                    if not releases:
                        skipped_no_releases += 1
                        logger.error(f"Skipping {w2p_title} (ID: {w2p_id}) - no W2P releases found in DMM. Entry keys: {list(w2p_entry.keys())}, Entry structure: {w2p_entry}")
                        continue
                    
                    # Find the matching watchlist item - try ID first, then title
                    d = ident_to_watchlist_item.get(str(w2p_id)) if w2p_id else None
                    if not d and w2p_title:
                        # Fallback: try matching by title
                        for watchlist_item in items_to_harvest:
                            if watchlist_item.get("title", "").lower() == w2p_title.lower():
                                d = watchlist_item
                                logger.warning(f"Matched W2P result {w2p_title} to watchlist item by title (ID match failed)")
                                break
                    
                    if not d:
                        skipped_no_match += 1
                        logger.error(f"Could not match W2P result {w2p_id} ({w2p_title}) to watchlist item. Available IDs: {list(ident_to_watchlist_item.keys())[:10]}, Available titles: {[i.get('title') for i in items_to_harvest[:5]]}")
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
            
                logger.info(f"W2P processing summary: {matched_count} matched, {skipped_no_releases} skipped (no releases), {skipped_no_match} skipped (no match)")
                if matched_count == 0:
                    if w2p_results:
                        logger.warning(f"W2P returned {len(w2p_results)} results but none were usable. {skipped_no_releases} had no releases, {skipped_no_match} couldn't be matched.")
                    else:
                        logger.warning(f"W2P returned no results for {len(items_to_harvest)} watchlist items. Check W2P logs to see if items were found in DMM.")

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
