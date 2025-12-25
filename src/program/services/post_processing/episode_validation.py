"""Episode Validation Service

Validates that seasons have all expected episodes and triggers W2P searches for missing ones.
"""

from typing import Generator
import httpx
from kink import di
from loguru import logger

from program.apis.tmdb_api import TMDBApi
from program.media.item import MediaItem, Season, Episode, Show
from program.media.state import States
from program.settings.manager import settings_manager
from program.db.db_functions import get_item_by_external_id
from program.types import Event
from program.managers.event_manager import EventManager


class EpisodeValidationService:
    """Validates episode completeness and triggers W2P searches for missing episodes."""

    @staticmethod
    def should_submit(item: MediaItem) -> bool:
        """Check if this item should be validated."""
        # Only validate seasons and shows that are completed
        if item.type == "season":
            return item.last_state == States.Completed
        elif item.type == "show":
            # Check if any season is completed
            return any(s.last_state == States.Completed for s in item.seasons)
        return False

    def __init__(self):
        self.key = "episode_validation"
        self.tmdb_api = None
        self.w2p_settings = None
        try:
            self.tmdb_api = di[TMDBApi]
        except Exception:
            logger.warning("TMDBApi not available, episode validation will be limited")
        
        # Get W2P settings
        try:
            self.w2p_settings = getattr(settings_manager.settings.content, 'watchlist2plex', None)
        except Exception:
            logger.warning("watchlist2plex settings not available, cannot trigger W2P searches for missing episodes")

    def _get_expected_episode_count(self, show: Show, season: Season) -> int | None:
        """Get expected episode count for a season from TMDB."""
        if not self.tmdb_api or not show.tmdb_id:
            return None
        
        try:
            result = self.tmdb_api.get_tv_season_details(int(show.tmdb_id), season.number)
            if result and result.data:
                episodes = getattr(result.data, "episodes", None)
                if episodes:
                    return len(episodes)
        except Exception as e:
            logger.debug(f"Failed to get expected episode count for {show.log_string} S{season.number:02d}: {e}")
        
        return None

    def _get_missing_episodes(self, season: Season) -> list[int]:
        """Get list of missing episode numbers for a season."""
        if not season.episodes:
            return []
        
        # Get all episode numbers we have
        existing_episode_numbers = {ep.number for ep in season.episodes if ep.number}
        
        # Find the max episode number we have
        if not existing_episode_numbers:
            return []
        
        max_episode = max(existing_episode_numbers)
        
        # Check for missing episodes (gaps in sequence)
        missing = []
        for ep_num in range(1, max_episode + 1):
            if ep_num not in existing_episode_numbers:
                missing.append(ep_num)
        
        return missing

    def _call_w2p_for_episodes(self, show: Show, season: Season, missing_episodes: list[int]) -> dict[int, list]:
        """Call W2P to search for specific missing episodes. Returns dict mapping episode_number -> releases."""
        if not self.w2p_settings or not getattr(self.w2p_settings, 'enabled', False):
            logger.debug("W2P not enabled, skipping episode search")
            return {}
        
        if not show.tmdb_id and not show.imdb_id:
            logger.debug(f"Cannot call W2P for {show.log_string} - missing tmdb_id or imdb_id")
            return {}
        
        # Build payload for W2P
        items_payload = []
        for ep_num in missing_episodes:
            identifier = show.imdb_id or show.tmdb_id
            items_payload.append({
                "id": str(identifier),
                "title": show.title or "",
                "year": show.year,
                "type": "show",
                "season": season.number,
                "episode": ep_num,
            })
        
        if not items_payload:
            return {}
        
        # Get W2P URL
        w2p_url = getattr(self.w2p_settings, 'url', 'http://localhost:8080/riven/harvest-item') or 'http://localhost:8080/riven/harvest-item'
        base_url = w2p_url.rstrip("/")
        if base_url.endswith("/watchlist"):
            harvest_url = base_url.replace("/watchlist", "/harvest-item")
        elif not base_url.endswith("/harvest-item"):
            harvest_url = f"{base_url}/harvest-item"
        else:
            harvest_url = base_url
        
        headers = {}
        auth_name = getattr(self.w2p_settings, 'auth_header_name', '') or ''
        auth_value = getattr(self.w2p_settings, 'auth_header_value', '') or ''
        if auth_name and auth_value:
            headers[auth_name] = auth_value
        
        episode_releases = {}
        try:
            logger.info(f"Calling W2P to search for {len(missing_episodes)} missing episodes: {show.log_string} S{season.number:02d} E{', '.join(f'E{ep:02d}' for ep in missing_episodes)}")
            with httpx.Client(timeout=120.0) as client:
                resp = client.post(
                    harvest_url,
                    json={"items": items_payload},
                    headers=headers,
                )
                resp.raise_for_status()
                data = resp.json()
                items_list = data.get("items", [])
                logger.info(f"W2P returned {len(items_list)} results for missing episodes")
                
                # Map releases to episode numbers
                for entry in items_list:
                    item_data = entry.get("item", entry)
                    releases = entry.get("releases", [])
                    ep_num = item_data.get("episode")
                    if ep_num and releases:
                        episode_releases[ep_num] = releases
                        logger.info(f"W2P found {len(releases)} releases for {show.log_string} S{season.number:02d}E{ep_num:02d}")
                
                return episode_releases
        except Exception as e:
            logger.warning(f"Failed to call W2P for missing episodes: {e}")
            return {}

    def _queue_missing_episodes(self, show: Show, season: Season, missing_episodes: list[int], episode_releases: dict[int, list]):
        """Queue missing episodes for re-processing and store W2P releases."""
        from program.program import riven
        from program.db.db import db
        
        for ep_num in missing_episodes:
            # Check if episode already exists in database
            existing_episode = None
            for ep in season.episodes:
                if ep.number == ep_num:
                    existing_episode = ep
                    break
            
            releases = episode_releases.get(ep_num, [])
            
            if existing_episode:
                # Episode exists - update with W2P releases if available
                if releases:
                    with db.Session() as session:
                        session.merge(existing_episode)
                        aliases = getattr(existing_episode, "aliases", {}) or {}
                        aliases["w2p_releases"] = releases
                        existing_episode.set("aliases", aliases)
                        # Clear scraped_at to trigger re-scraping with new W2P releases
                        existing_episode.set("scraped_at", None)
                        # Reset state to Indexed to ensure it's re-queued for processing
                        existing_episode.store_state(States.Indexed)
                        session.commit()
                        logger.info(f"Updated episode {show.log_string} S{season.number:02d}E{ep_num:02d} with {len(releases)} W2P releases")
                
                # Queue for re-processing
                if existing_episode.id:
                    event = Event(emitted_by=self, item_id=str(existing_episode.id))
                    riven.em.add_event_to_queue(event)
                    logger.info(f"Queued existing episode {show.log_string} S{season.number:02d}E{ep_num:02d} for re-processing")
            else:
                # Episode doesn't exist - create it with W2P releases
                from program.media.item import Episode
                from datetime import datetime
                
                episode_item = {
                    "number": ep_num,
                    "title": f"Episode {ep_num}",
                    "type": "episode",
                    "requested_at": datetime.now(),
                    "requested_by": self.key,
                    "aliases": {"w2p_releases": releases} if releases else {},
                    "is_anime": getattr(season, "is_anime", False),
                }
                
                new_episode = Episode(episode_item)
                new_episode.parent = season
                
                # Save to database first
                with db.Session() as session:
                    session.add(new_episode)
                    session.commit()
                    session.refresh(new_episode)
                
                # Queue for processing
                if new_episode.id:
                    event = Event(emitted_by=self, item_id=str(new_episode.id))
                    riven.em.add_event_to_queue(event)
                    logger.info(f"Created and queued new episode {show.log_string} S{season.number:02d}E{ep_num:02d} for processing ({len(releases)} W2P releases)")

    def run(self, item: MediaItem) -> Generator[MediaItem, None, None]:
        """
        Validate episode completeness for a season or show.
        
        For seasons: Check if all expected episodes are present
        For shows: Check all completed seasons
        """
        if item.type == "season":
            seasons_to_check = [item]
        elif item.type == "show":
            # Only check completed seasons
            seasons_to_check = [s for s in item.seasons if s.last_state == States.Completed]
        else:
            yield item
            return
        
        if not seasons_to_check:
            yield item
            return
        
        # Get the show (parent of season)
        show = item if item.type == "show" else item.show
        
        if not show:
            logger.warning(f"Cannot validate episodes - no show found for {item.log_string}")
            yield item
            return
        
        for season in seasons_to_check:
            # Get expected episode count from TMDB
            expected_count = self._get_expected_episode_count(show, season)
            
            if expected_count is None:
                logger.debug(f"Cannot get expected episode count for {show.log_string} S{season.number:02d}, skipping validation")
                continue
            
            # Get actual episode count
            actual_episodes = [ep for ep in season.episodes if ep.number]
            actual_count = len(actual_episodes)
            
            logger.info(f"Episode validation for {show.log_string} S{season.number:02d}: {actual_count}/{expected_count} episodes")
            
            if actual_count < expected_count:
                # Find missing episodes
                missing_episodes = self._get_missing_episodes(season)
                
                # Also check if we're missing episodes beyond the max we have
                if actual_count < expected_count:
                    max_existing = max((ep.number for ep in actual_episodes), default=0)
                    for ep_num in range(max_existing + 1, expected_count + 1):
                        if ep_num not in missing_episodes:
                            missing_episodes.append(ep_num)
                
                if missing_episodes:
                    logger.warning(
                        f"Missing {len(missing_episodes)} episodes for {show.log_string} S{season.number:02d}: "
                        f"E{', '.join(f'E{ep:02d}' for ep in sorted(missing_episodes))}"
                    )
                    
                    # Call W2P to search for missing episodes
                    episode_releases = self._call_w2p_for_episodes(show, season, missing_episodes)
                    if episode_releases:
                        # Queue missing episodes for re-processing with W2P releases
                        self._queue_missing_episodes(show, season, missing_episodes, episode_releases)
                    else:
                        # Still queue episodes even if W2P didn't return releases (might find via scrapers)
                        self._queue_missing_episodes(show, season, missing_episodes, {})
        
        yield item

