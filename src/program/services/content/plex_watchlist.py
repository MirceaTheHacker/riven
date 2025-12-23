"""Plex Watchlist Module"""

from typing import Generator

import httpx
from kink import di
from loguru import logger
from requests import HTTPError

from program.apis.plex_api import PlexAPI
from program.apis.tmdb_api import TMDBApi
from program.db.db_functions import get_item_by_external_id, item_exists_by_any_id
from program.db.db import db
from program.media.item import MediaItem
from program.media.state import States
from program.settings.manager import settings_manager


class PlexWatchlist:
    """Class for managing Plex Watchlists"""

    def __init__(self):
        self.key = "plex_watchlist"
        self.settings = settings_manager.settings.content.plex_watchlist
        # Handle case where watchlist2plex might not exist in older settings
        self.w2p_settings = getattr(settings_manager.settings.content, 'watchlist2plex', None)
        if self.w2p_settings is None:
            # Fallback: create a minimal config object if watchlist2plex doesn't exist
            # Use a simple object with the attributes we need instead of importing
            class MinimalW2PSettings:
                enabled = False
                url = "http://localhost:8080/riven/harvest-item"
                auth_header_name = ""
                auth_header_value = ""
                force = False
                limit = -1
                update_interval = 900
            self.w2p_settings = MinimalW2PSettings()
            logger.warning("watchlist2plex settings not found in ContentModel, using minimal defaults")
        self.api = None
        self.tmdb_api = None
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
            # Initialize TMDB API for fetching titles
            try:
                self.tmdb_api = di[TMDBApi]
            except Exception:
                logger.warning("TMDBApi not available in DI container, title fetching will be limited")
                self.tmdb_api = None
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

    def _fetch_title_from_tmdb(self, item: dict) -> str | None:
        """Fetch title from TMDB API using available IDs."""
        if not self.tmdb_api:
            return None
        
        item_type = item.get("type", "movie")
        tmdb_id = item.get("tmdb_id")
        imdb_id = item.get("imdb_id")
        
        try:
            if item_type == "movie":
                if tmdb_id:
                    result = self.tmdb_api.get_movie_details(tmdb_id)
                    if result and result.data and hasattr(result.data, "title"):
                        return result.data.title
                elif imdb_id:
                    # Lookup via IMDB ID
                    results = self.tmdb_api.get_from_external_id("imdb_id", imdb_id)
                    if results and results.data and hasattr(results.data, "movie_results"):
                        movie_results = results.data.movie_results
                        if movie_results:
                            return movie_results[0].title if hasattr(movie_results[0], "title") else None
            elif item_type == "show":
                if tmdb_id:
                    result = self.tmdb_api.get_tv_details(tmdb_id)
                    if result and result.data and hasattr(result.data, "name"):
                        return result.data.name
                elif imdb_id:
                    # Lookup via IMDB ID
                    results = self.tmdb_api.get_from_external_id("imdb_id", imdb_id)
                    if results and results.data and hasattr(results.data, "tv_results"):
                        tv_results = results.data.tv_results
                        if tv_results:
                            return tv_results[0].name if hasattr(tv_results[0], "name") else None
        except Exception as e:
            logger.debug(f"Failed to fetch title from TMDB for {item.get('imdb_id') or item.get('tmdb_id')}: {e}")
        
        return None

    def _build_w2p_payload(self, watchlist_items: list[dict[str, str]]) -> list[dict]:
        payload = []
        for idx, d in enumerate(watchlist_items):
            title = d.get("title")
            item_type = d.get("type") or "movie"
            identifier = d.get("imdb_id") or d.get("tmdb_id") or d.get("tvdb_id") or title
            if not title or not identifier:
                logger.warning(f"Skipping watchlist item #{idx} in payload - missing title or identifier. Item keys: {list(d.keys())}, Item: {d}")
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
        if not payload and watchlist_items:
            logger.error(f"Failed to build W2P payload from {len(watchlist_items)} items. First item structure: {watchlist_items[0] if watchlist_items else 'N/A'}")
        return payload

    def _call_w2p(self, items_payload: list[dict]) -> dict[str, dict]:
        if not self.w2p_settings:
            logger.warning("W2P settings not available")
            return {}
        if not getattr(self.w2p_settings, 'enabled', False):
            logger.warning("W2P is not enabled in settings")
            return {}
        if not items_payload:
            logger.warning("W2P payload is empty, skipping call")
            return {}

        headers = {}
        auth_name = getattr(self.w2p_settings, 'auth_header_name', '') or ''
        auth_value = getattr(self.w2p_settings, 'auth_header_value', '') or ''
        if auth_name and auth_value:
            headers[auth_name] = auth_value

        # Use /riven/harvest-item endpoint which accepts items in JSON body
        # The default URL might be /riven/watchlist, so we need to construct the correct endpoint
        w2p_url = getattr(self.w2p_settings, 'url', 'http://localhost:8080/riven/harvest-item') or 'http://localhost:8080/riven/harvest-item'
        base_url = w2p_url.rstrip("/")
        if base_url.endswith("/watchlist"):
            harvest_url = base_url.replace("/watchlist", "/harvest-item")
        elif not base_url.endswith("/harvest-item"):
            harvest_url = f"{base_url}/harvest-item"
        else:
            harvest_url = base_url

        logger.info(f"Calling W2P at URL: {harvest_url} with {len(items_payload)} items")
        logger.debug(f"W2P request payload: {items_payload}")
        logger.debug(f"W2P request headers: {headers}")

        try:
            with httpx.Client(timeout=120.0) as client:  # Increased timeout for browser automation
                resp = client.post(
                    harvest_url,
                    json={"items": items_payload},
                    headers=headers,
                )
                logger.debug(f"W2P response status: {resp.status_code}")
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
        except httpx.TimeoutException as e:
            logger.error(f"W2P request timed out after 120s to {harvest_url}: {e}")
            return {}
        except httpx.ConnectError as e:
            logger.error(f"W2P connection error - cannot reach {harvest_url}. Is W2P running? Error: {e}")
            return {}
        except httpx.HTTPStatusError as e:
            logger.error(f"W2P returned HTTP error {e.response.status_code} for {harvest_url}: {e.response.text}")
            return {}
        except Exception as e:
            logger.error(f"Failed calling Watchlist2Plex harvest endpoint {harvest_url}: {e}", exc_info=True)
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
                # Check if item exists in database (for logging purposes and to get title)
                existing_item = None
                if item.get("imdb_id"):
                    existing_item = get_item_by_external_id(imdb_id=item["imdb_id"])
                if not existing_item and item.get("tmdb_id"):
                    existing_item = get_item_by_external_id(tmdb_id=item["tmdb_id"])
                if not existing_item and item.get("tvdb_id"):
                    existing_item = get_item_by_external_id(tvdb_id=item["tvdb_id"])
                
                # Get title from database if item exists
                if existing_item:
                    # Use title from database if available
                    if not item.get("title") and hasattr(existing_item, "title") and existing_item.title:
                        item["title"] = existing_item.title
                    # Check if item already has W2P releases stored (for logging)
                    aliases = getattr(existing_item, "aliases", {}) or {}
                    w2p_releases = aliases.get("w2p_releases") or []
                    if w2p_releases:
                        logger.debug(f"Including {item.get('title', 'unknown')} - exists with {len(w2p_releases)} W2P releases, will refresh from W2P")
                    else:
                        logger.debug(f"Including {item.get('title', 'unknown')} - exists in database but has no W2P releases (will check for better quality)")
                else:
                    logger.debug(f"Including {item.get('title', 'unknown')} - new item, will fetch from W2P")
                
                # If still no title, fetch from TMDB
                if not item.get("title"):
                    fetched_title = self._fetch_title_from_tmdb(item)
                    if fetched_title:
                        item["title"] = fetched_title
                        logger.debug(f"Fetched title '{fetched_title}' from TMDB for {item.get('imdb_id') or item.get('tmdb_id')}")
                    else:
                        # Last resort: use identifier as fallback (W2P needs a title for searching)
                        identifier = item.get("imdb_id") or item.get("tmdb_id") or item.get("tvdb_id")
                        if identifier:
                            item["title"] = identifier
                            logger.warning(f"Watchlist item missing title and TMDB fetch failed, using identifier '{identifier}' as fallback")
                
                # Log item structure for debugging
                logger.debug(f"Item to harvest structure: keys={list(item.keys())}, title={item.get('title')}, imdb={item.get('imdb_id')}, tmdb={item.get('tmdb_id')}, tvdb={item.get('tvdb_id')}")
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

                    # Check if item already exists in database
                    existing_item = None
                    if d.get("imdb_id"):
                        existing_item = get_item_by_external_id(imdb_id=d["imdb_id"])
                    if not existing_item and d.get("tmdb_id"):
                        existing_item = get_item_by_external_id(tmdb_id=d["tmdb_id"])
                    if not existing_item and d.get("tvdb_id"):
                        existing_item = get_item_by_external_id(tvdb_id=d["tvdb_id"])

                    if existing_item:
                        # Update existing item with W2P releases
                        current_aliases = getattr(existing_item, "aliases", {}) or {}
                        current_aliases["w2p_releases"] = releases
                        existing_item.set("aliases", current_aliases)
                        
                        # Try to correct the item's year from W2P releases if there's a clear mismatch
                        # Extract years from W2P release titles (they typically contain the year)
                        import re
                        years_from_releases = []
                        for rel in releases:
                            title = rel.get("title", "") or rel.get("raw_title", "")
                            if isinstance(title, str):
                                # Look for 4-digit years in the title (1900-2099)
                                year_matches = re.findall(r'\b(19\d{2}|20\d{2})\b', title)
                                if year_matches:
                                    try:
                                        year = int(year_matches[0])
                                        if 1900 <= year <= 2099:
                                            years_from_releases.append(year)
                                    except (ValueError, IndexError):
                                        pass
                        
                        # If we found years in releases and there's a clear consensus, update the item
                        if years_from_releases:
                            from collections import Counter
                            year_counts = Counter(years_from_releases)
                            most_common_year, count = year_counts.most_common(1)[0]
                            # If at least 50% of releases agree on the year, and it differs from item's year
                            if count >= len(years_from_releases) * 0.5:
                                item_year = None
                                if hasattr(existing_item, "aired_at") and existing_item.aired_at:
                                    item_year = existing_item.aired_at.year if hasattr(existing_item.aired_at, "year") else None
                                elif hasattr(existing_item, "year") and existing_item.year:
                                    item_year = existing_item.year
                                
                                if item_year and item_year != most_common_year:
                                    # Update the year
                                    from datetime import datetime
                                    if hasattr(existing_item, "aired_at"):
                                        # Update aired_at to the correct year (keeping month/day if available, otherwise Jan 1)
                                        old_aired_at = existing_item.aired_at
                                        if isinstance(old_aired_at, datetime):
                                            new_aired_at = datetime(most_common_year, old_aired_at.month, old_aired_at.day)
                                        else:
                                            new_aired_at = datetime(most_common_year, 1, 1)
                                        existing_item.set("aired_at", new_aired_at)
                                    if hasattr(existing_item, "year"):
                                        existing_item.set("year", most_common_year)
                                    logger.info(f"Corrected year for {d.get('title')} from {item_year} to {most_common_year} based on W2P releases ({count}/{len(years_from_releases)} releases agree)")
                        
                        # Clear scraped_at and reset state to Indexed to trigger re-scraping with new W2P releases
                        existing_item.set("scraped_at", None)
                        existing_item.store_state(States.Indexed)
                        # Save the update
                        with db.Session() as session:
                            session.merge(existing_item)
                            session.commit()
                        logger.info(f"Updated existing item {d.get('title')} (ID: {existing_item.id}) with {len(releases)} W2P releases and reset to Indexed state to trigger re-scraping")
                        # Yield the existing item so it gets re-queued for scraping
                        items_to_yield.append(existing_item)
                    else:
                        # Build item data for new item using the watchlist item's IDs
                        if d.get("tvdb_id") and not d.get("tmdb_id"):
                            item_data = {"tvdb_id": d["tvdb_id"], "requested_by": self.key}
                        elif d.get("tmdb_id"):
                            item_data = {"tmdb_id": d["tmdb_id"], "requested_by": self.key}
                        else:
                            # fallback to imdb-only
                            item_data = {"imdb_id": d.get("imdb_id"), "requested_by": self.key}

                        item_data["aliases"] = {"w2p_releases": releases}
                        items_to_yield.append(MediaItem(item_data))
                        logger.info(f"Created new item {d.get('title')} with {len(releases)} releases from W2P")
                    
                    matched_count += 1
            
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
            # Filter out only NEW items that already exist (don't filter existing items we updated)
            filtered_items = []
            for item in items_to_yield:
                # If item has an ID, it's an existing item we updated - always include it
                if hasattr(item, 'id') and item.id:
                    filtered_items.append(item)
                # If item doesn't have an ID, it's a new item - check if it already exists
                elif not item_exists_by_any_id(
                    imdb_id=item.imdb_id, tvdb_id=item.tvdb_id, tmdb_id=item.tmdb_id
                ):
                    filtered_items.append(item)
            items_to_yield = filtered_items

        logger.info(f"Fetched {len(items_to_yield)} items from plex watchlist ({sum(1 for i in items_to_yield if hasattr(i, 'id') and i.id)} updated, {sum(1 for i in items_to_yield if not (hasattr(i, 'id') and i.id))} new)")
        yield items_to_yield
