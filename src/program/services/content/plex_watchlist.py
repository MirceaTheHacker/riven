"""Plex Watchlist Module"""

from datetime import datetime, timedelta
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
            logger.debug("TMDB API not available for title fetch")
            return None
        
        item_type = item.get("type", "movie")
        tmdb_id = item.get("tmdb_id")
        imdb_id = item.get("imdb_id")
        
        # Try both TMDB ID and IMDb ID to maximize chances of success
        try:
            if item_type == "movie":
                # Try TMDB ID first (more direct)
                if tmdb_id:
                    try:
                        result = self.tmdb_api.get_movie_details(tmdb_id)
                        if result and result.data and hasattr(result.data, "title"):
                            logger.debug(f"Fetched title '{result.data.title}' from TMDB using TMDB ID {tmdb_id}")
                            return result.data.title
                    except Exception as e:
                        logger.debug(f"TMDB ID lookup failed for {tmdb_id}: {e}")
                
                # Fallback to IMDb ID lookup
                if imdb_id:
                    try:
                        results = self.tmdb_api.get_from_external_id("imdb_id", imdb_id)
                        if results and results.data and hasattr(results.data, "movie_results"):
                            movie_results = results.data.movie_results
                            if movie_results:
                                title = movie_results[0].title if hasattr(movie_results[0], "title") else None
                                if title:
                                    logger.debug(f"Fetched title '{title}' from TMDB using IMDb ID {imdb_id}")
                                    return title
                    except Exception as e:
                        logger.debug(f"IMDb ID lookup failed for {imdb_id}: {e}")
            elif item_type == "show":
                # Try TMDB ID first (more direct)
                if tmdb_id:
                    try:
                        result = self.tmdb_api.get_tv_details(tmdb_id)
                        if result and result.data and hasattr(result.data, "name"):
                            logger.debug(f"Fetched title '{result.data.name}' from TMDB using TMDB ID {tmdb_id}")
                            return result.data.name
                    except Exception as e:
                        logger.debug(f"TMDB ID lookup failed for {tmdb_id}: {e}")
                
                # Fallback to IMDb ID lookup
                if imdb_id:
                    try:
                        results = self.tmdb_api.get_from_external_id("imdb_id", imdb_id)
                        if results and results.data and hasattr(results.data, "tv_results"):
                            tv_results = results.data.tv_results
                            if tv_results:
                                title = tv_results[0].name if hasattr(tv_results[0], "name") else None
                                if title:
                                    logger.debug(f"Fetched title '{title}' from TMDB using IMDb ID {imdb_id}")
                                    return title
                    except Exception as e:
                        logger.debug(f"IMDb ID lookup failed for {imdb_id}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error fetching title from TMDB for {item.get('imdb_id') or item.get('tmdb_id')}: {e}")
        
        return None

    def _build_w2p_payload(self, watchlist_items: list[dict[str, str]]) -> list[dict]:
        payload = []
        for idx, d in enumerate(watchlist_items):
            title = d.get("title")
            item_type = d.get("type") or "movie"
            identifier = d.get("imdb_id") or d.get("tmdb_id") or d.get("tvdb_id") or title
            
            # Require at least an identifier (IMDb/TMDB/TVDB ID) to proceed
            if not identifier:
                logger.warning(f"Skipping watchlist item #{idx} in payload - missing identifier. Item keys: {list(d.keys())}, Item: {d}")
                continue
            
            # If no title but we have an IMDb ID, use the IMDb ID as title (W2P can handle direct navigation by IMDb ID)
            # This allows W2P to use direct navigation instead of searching
            if not title:
                if d.get("imdb_id") and d.get("imdb_id").startswith("tt"):
                    # Use IMDb ID as title - W2P will detect it and use direct navigation
                    title = d.get("imdb_id")
                    logger.debug(f"Using IMDb ID '{title}' as title for W2P direct navigation (item #{idx})")
                else:
                    # For non-IMDb IDs, we need a title for searching
                    logger.warning(f"Skipping watchlist item #{idx} in payload - missing title and no IMDb ID for direct navigation. Item keys: {list(d.keys())}, Item: {d}")
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

        logger.info(f"🔍 Calling W2P at URL: {harvest_url} with {len(items_payload)} items")
        logger.info(f"📦 W2P request payload: {[{'id': p.get('id'), 'title': p.get('title'), 'type': p.get('type'), 'season': p.get('season'), 'episode': p.get('episode')} for p in items_payload]}")
        logger.debug(f"W2P request headers: {headers}")

        try:
            # Calculate timeout based on number of items
            # Each item can take 30-60 seconds (especially shows with multiple seasons)
            # Add buffer for network monitoring and processing
            base_timeout = 60.0  # Base timeout per item
            timeout_per_item = 90.0  # Additional seconds per item
            total_timeout = base_timeout + (len(items_payload) * timeout_per_item)
            # Cap at 10 minutes (600 seconds) to prevent extremely long waits
            total_timeout = min(total_timeout, 600.0)
            
            logger.info(f"W2P timeout set to {total_timeout:.0f}s for {len(items_payload)} items")
            with httpx.Client(timeout=total_timeout) as client:
                resp = client.post(
                    harvest_url,
                    json={"items": items_payload},
                    headers=headers,
                )
                logger.debug(f"W2P response status: {resp.status_code}")
                resp.raise_for_status()
                data = resp.json()
                logger.info(f"✅ W2P harvest returned {len(data.get('items', []))} items")
                logger.info(f"📊 W2P harvest response: status={data.get('status')}, processed_count={data.get('processed_count')}, items_count={len(data.get('items', []))}")
                # Log releases count for each item
                for idx, item_entry in enumerate(data.get('items', [])[:3]):  # Log first 3 items
                    item_data = item_entry.get('item', item_entry)
                    releases = item_entry.get('releases', [])
                    logger.info(f"   Item {idx+1}: {item_data.get('title', 'unknown')} - {len(releases)} releases")
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
            # Call W2P for watchlist items to get latest releases
            # Skip items that are already completed with streams to avoid unnecessary calls
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
                    
                    # Check item state and W2P releases
                    item_state = existing_item.state
                    is_completed = item_state == States.Completed
                    
                    # Check if item already has W2P releases stored
                    aliases = getattr(existing_item, "aliases", {}) or {}
                    w2p_releases = aliases.get("w2p_releases") or []
                    
                    # Skip completed items that HAVE W2P releases (they're done with W2P)
                    if is_completed and w2p_releases:
                        logger.debug(f"Skipping {item.get('title', 'unknown')} - already completed with {len(w2p_releases)} W2P releases, no need to refresh from W2P")
                        continue
                    
                    # For completed items WITHOUT W2P releases, check retry count and cooldown to prevent infinite loops
                    # These items need W2P data for better quality, but we don't want to spam W2P every 60 seconds
                    if is_completed and not w2p_releases:
                        w2p_attempt_count = aliases.get("w2p_attempt_count", 0)
                        # Max 3 attempts to prevent infinite loops
                        if w2p_attempt_count >= 3:
                            logger.debug(f"Skipping {item.get('title', 'unknown')} - completed but no W2P releases after {w2p_attempt_count} attempts (max 3)")
                            continue
                        
                        last_w2p_attempt = aliases.get("w2p_last_attempt")
                        if last_w2p_attempt:
                            try:
                                # Parse the timestamp (stored as ISO string)
                                attempt_time = datetime.fromisoformat(last_w2p_attempt.replace('Z', '+00:00'))
                                # Check if it's been less than 24 hours since last attempt
                                if datetime.now(attempt_time.tzinfo) - attempt_time < timedelta(hours=24):
                                    logger.debug(f"Skipping {item.get('title', 'unknown')} - completed but no W2P releases, last attempt was {attempt_time.strftime('%Y-%m-%d %H:%M')}, cooldown active (24h)")
                                    continue
                            except (ValueError, AttributeError):
                                # Invalid timestamp format, treat as old attempt and retry
                                pass
                        # Include it - either never attempted or cooldown expired
                        logger.info(f"✅ Including {item.get('title', 'unknown')} - completed but no W2P releases (attempt {w2p_attempt_count + 1}/3, will fetch W2P data for better quality)")
                    
                    # Skip items that have W2P releases and are in a processing state (not completed)
                    # This prevents loops where items are reset to Indexed and then immediately re-harvested
                    if w2p_releases and not is_completed:
                        logger.info(f"⏭️  Skipping {item.get('title', 'unknown')} - has {len(w2p_releases)} W2P releases and is in {item_state} state (being processed), will not refresh")
                        continue
                    
                    # Log why we're including the item (for non-completed items)
                    if not is_completed:
                        logger.info(f"✅ Including {item.get('title', 'unknown')} - state={item_state}, has_w2p_releases={len(w2p_releases) > 0}")
                else:
                    logger.debug(f"Including {item.get('title', 'unknown')} - new item, will fetch from W2P")
                
                # If still no title, fetch from TMDB
                if not item.get("title"):
                    fetched_title = self._fetch_title_from_tmdb(item)
                    if fetched_title:
                        item["title"] = fetched_title
                        logger.debug(f"Fetched title '{fetched_title}' from TMDB for {item.get('imdb_id') or item.get('tmdb_id')}")
                    else:
                        # For IMDb IDs, W2P can use direct navigation, so we can still proceed
                        # For other IDs, we need a title for searching
                        imdb_id = item.get("imdb_id")
                        if imdb_id and imdb_id.startswith("tt"):
                            # W2P can handle IMDb IDs directly via navigation, so we'll let it proceed
                            # The title will be set to the IMDb ID in _build_w2p_payload if needed
                            logger.info(f"Watchlist item missing title but has IMDb ID '{imdb_id}' - W2P can use direct navigation")
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
                
                # Store W2P attempt timestamp for all items being sent to W2P
                # This allows us to implement cooldown periods to prevent infinite loops
                attempt_timestamp = datetime.now().isoformat()
                for item in items_to_harvest:
                    # Find the existing item in database to update timestamp
                    existing_item = None
                    if item.get("imdb_id"):
                        existing_item = get_item_by_external_id(imdb_id=item["imdb_id"])
                    if not existing_item and item.get("tmdb_id"):
                        existing_item = get_item_by_external_id(tmdb_id=item["tmdb_id"])
                    if not existing_item and item.get("tvdb_id"):
                        existing_item = get_item_by_external_id(tvdb_id=item["tvdb_id"])
                    
                    if existing_item:
                        # Update the timestamp and attempt count in aliases
                        current_aliases = getattr(existing_item, "aliases", {}) or {}
                        current_aliases["w2p_last_attempt"] = attempt_timestamp
                        # Increment attempt count if no W2P releases exist
                        if not current_aliases.get("w2p_releases"):
                            current_aliases["w2p_attempt_count"] = current_aliases.get("w2p_attempt_count", 0) + 1
                        else:
                            # Reset attempt count if we have releases
                            current_aliases["w2p_attempt_count"] = 0
                        existing_item.set("aliases", current_aliases)
                        # Save immediately so timestamp is stored even if W2P call fails
                        with db.Session() as session:
                            session.merge(existing_item)
                            session.commit()
                
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
                    needs_rd_library_check = w2p_entry.get("needs_rd_library_check", False)
                    
                    logger.warning(f"Processing W2P result: title={w2p_title}, id={w2p_id}, releases_count={len(releases)}, needs_rd_library_check={needs_rd_library_check}, entry_keys={list(w2p_entry.keys())}, item_keys={list(w2p_item.keys())}")
                    
                    # Edge case: W2P clicked Instant RD buttons but got no releases
                    # Query RD library directly to find the torrents that were just added
                    if not releases and needs_rd_library_check:
                        logger.info(f"🔍 Edge case detected for {w2p_title}: W2P clicked Instant RD buttons but got no releases. Querying RD library...")
                        try:
                            from program.services.downloaders import Downloader
                            downloader = Downloader()
                            if downloader.service and hasattr(downloader.service, 'get_downloads'):
                                rd_downloads = downloader.service.get_downloads()
                                # Filter downloads by title (fuzzy match)
                                title_lower = w2p_title.lower()
                                matching_downloads = []
                                for dl in rd_downloads:
                                    dl_name = getattr(dl, 'filename', '') or getattr(dl, 'name', '') or ''
                                    if title_lower in dl_name.lower() or dl_name.lower() in title_lower:
                                        matching_downloads.append(dl)
                                
                                if matching_downloads:
                                    logger.info(f"✅ Found {len(matching_downloads)} matching torrent(s) in RD library for {w2p_title}")
                                    # Convert RD downloads to W2P release format
                                    for dl in matching_downloads:
                                        dl_name = getattr(dl, 'filename', '') or getattr(dl, 'name', '')
                                        dl_size = getattr(dl, 'bytes', 0) or 0
                                        dl_hash = getattr(dl, 'hash', '') or ''
                                        releases.append({
                                            "title": dl_name,
                                            "size_bytes": dl_size,
                                            "infohash": dl_hash,
                                            "source_label": "rd-library",
                                            "already_in_rd": True,
                                        })
                                else:
                                    logger.warning(f"⚠️ No matching torrents found in RD library for {w2p_title}")
                        except Exception as e:
                            logger.error(f"❌ Failed to query RD library for {w2p_title}: {e}", exc_info=True)
                    
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
                        # Check if W2P releases have actually changed
                        current_aliases = getattr(existing_item, "aliases", {}) or {}
                        existing_w2p_releases = current_aliases.get("w2p_releases") or []
                        
                        # Compare releases by creating sets of infohashes
                        existing_infohashes = {rel.get("infohash", "").lower() for rel in existing_w2p_releases if rel.get("infohash")}
                        new_infohashes = {rel.get("infohash", "").lower() for rel in releases if rel.get("infohash")}
                        releases_changed = existing_infohashes != new_infohashes
                        
                        # Check if item is already Completed with streams
                        item_state = existing_item.state
                        has_streams = hasattr(existing_item, "streams") and len(getattr(existing_item, "streams", [])) > 0
                        is_completed = item_state == States.Completed
                        
                        # Update existing item with W2P releases
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
                        year_corrected = False
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
                                    year_corrected = True
                        
                        # Only reset to Indexed if:
                        # 1. W2P releases have changed, OR
                        # 2. Year was corrected, OR
                        # 3. Item is not already Completed with streams
                        should_reset = releases_changed or year_corrected or (not is_completed or not has_streams)
                        
                        if should_reset:
                            # Clear scraped_at and reset state to Indexed to trigger re-scraping with new W2P releases
                            existing_item.set("scraped_at", None)
                            existing_item.store_state(States.Indexed)
                            # Save the update
                            with db.Session() as session:
                                session.merge(existing_item)
                                session.commit()
                            reason = []
                            if releases_changed:
                                reason.append("W2P releases changed")
                            if year_corrected:
                                reason.append("year corrected")
                            if not is_completed or not has_streams:
                                reason.append("item not completed or has no streams")
                            logger.info(f"Updated existing item {d.get('title')} (ID: {existing_item.id}) with {len(releases)} W2P releases and reset to Indexed state to trigger re-scraping (reason: {', '.join(reason)})")
                            # Yield the existing item so it gets re-queued for scraping
                            items_to_yield.append(existing_item)
                        else:
                            # Just save the updated releases without resetting state
                            with db.Session() as session:
                                session.merge(existing_item)
                                session.commit()
                            logger.debug(f"Updated existing item {d.get('title')} (ID: {existing_item.id}) with {len(releases)} W2P releases but did not reset state (already completed with streams, no changes)")
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
