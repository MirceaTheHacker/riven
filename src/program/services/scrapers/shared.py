"""Shared functions for scrapers."""

from typing import Dict, Set

from loguru import logger
from RTN import (
    RTN,
    ParsedData,
    Torrent,
    sort_torrents,
    BaseRankingModel,
    DefaultRanking,
)

from program.media.item import MediaItem
from program.media.stream import Stream
from program.settings.manager import settings_manager
from program.settings.models import RankingProfileSettings, RankingSettings, ScraperModel

scraping_settings: ScraperModel = settings_manager.settings.scraping
ranking_model: BaseRankingModel = DefaultRanking()
# Backward-compatible default RTN instance for routes that import `rtn` directly
# Uses the current default ranking profile at module import time.
rtn: RTN = RTN(settings_manager.settings.ranking.get_profile(), ranking_model)


def _get_ranking_context(
    library_path: str | None,
) -> tuple[RankingSettings, list[tuple[str, RankingProfileSettings]]]:
    ranking_settings: RankingSettings = settings_manager.settings.ranking
    path_value = str(library_path) if library_path else None
    profile_names = ranking_settings.get_profile_names_for_path(path_value)
    profiles = [(name, ranking_settings.get_profile(name)) for name in profile_names]
    return ranking_settings, profiles


def _parse_results(
    item: MediaItem,
    results: Dict[str, str],
    log_msg: bool = True,
    library_path: str | None = None,
) -> Dict[str, Stream]:
    """Parse the results from the scrapers into Torrent objects."""
    correct_title: str = item.get_top_title()

    ranking_settings, profile_contexts = _get_ranking_context(library_path)

    logger.debug(
        f"Processing {len(results)} results for {item.log_string} using profiles {[name for name, _ in profile_contexts]}"
    )

    combined_streams: Dict[str, Stream] = {}
    duplicates: set[str] = set()

    for profile_name, profile_settings in profile_contexts:
        # Build per-profile aliases respecting language exclusions
        aliases: Dict[str, list[str]] = (
            item.get_aliases() if scraping_settings.enable_aliases else {}
        )
        aliases = {
            k: v
            for k, v in aliases.items()
            if k not in profile_settings.languages.exclude
        }

        rtn = RTN(profile_settings, ranking_model)

        torrents: Set[Torrent] = set()
        processed_infohashes: Set[str] = set()

        for infohash, raw_title in results.items():
            if infohash in processed_infohashes:
                # Already processed this infohash within this profile
                continue
            try:
                torrent: Torrent = rtn.rank(
                    raw_title=raw_title,
                    infohash=infohash,
                    correct_title=correct_title,
                    remove_trash=profile_settings.options["remove_all_trash"],
                    aliases=aliases,
                )

                if item.type == "movie":
                    # If movie item, disregard torrents with seasons and episodes
                    if torrent.data.episodes or torrent.data.seasons:
                        logger.trace(
                            f"Skipping show torrent for movie {item.log_string}: {raw_title}"
                        )
                        continue

                if item.type == "show":
                    # make sure the torrent has at least 2 episodes (should weed out most junk)
                    if torrent.data.episodes and len(torrent.data.episodes) <= 2:
                        logger.trace(
                            f"Skipping torrent with too few episodes for {item.log_string}: {raw_title}"
                        )
                        continue

                    # make sure all of the item seasons are present in the torrent
                    if not all(
                        season.number in torrent.data.seasons for season in item.seasons
                    ):
                        logger.trace(
                            f"Skipping torrent with incorrect number of seasons for {item.log_string}: {raw_title}"
                        )
                        continue

                    if (
                        torrent.data.episodes
                        and not torrent.data.seasons
                        and len(item.seasons) == 1
                        and not all(
                            episode.number in torrent.data.episodes
                            for episode in item.seasons[0].episodes
                        )
                    ):
                        logger.trace(
                            f"Skipping torrent with incorrect number of episodes for {item.log_string}: {raw_title}"
                        )
                        continue

                if item.type == "season":
                    if torrent.data.seasons and item.number not in torrent.data.seasons:
                        logger.trace(
                            f"Skipping torrent with no seasons or incorrect season number for {item.log_string}: {raw_title}"
                        )
                        continue

                    # make sure the torrent has at least 2 episodes (should weed out most junk)
                    if torrent.data.episodes and len(torrent.data.episodes) <= 2:
                        logger.trace(
                            f"Skipping torrent with too few episodes for {item.log_string}: {raw_title}"
                        )
                        continue

                    # disregard torrents with incorrect season number
                    if item.number not in torrent.data.seasons:
                        logger.trace(
                            f"Skipping incorrect season torrent for {item.log_string}: {raw_title}"
                        )
                        continue

                    if torrent.data.episodes and not all(
                        episode.number in torrent.data.episodes
                        for episode in item.episodes
                    ):
                        logger.trace(
                            f"Skipping incorrect season torrent for not having all episodes {item.log_string}: {raw_title}"
                        )
                        continue

                if item.type == "episode":
                    # Disregard torrents with incorrect episode number logic:
                    skip = False
                    # If the torrent has episodes, but the episode number is not present
                    if torrent.data.episodes:
                        if (
                            item.number not in torrent.data.episodes
                            and item.absolute_number not in torrent.data.episodes
                        ):
                            skip = True
                    # If the torrent does not have episodes, but has seasons, and the parent season is not present
                    elif torrent.data.seasons:
                        if item.parent.number not in torrent.data.seasons:
                            skip = True
                    # If the torrent has neither episodes nor seasons, skip (junk)
                    else:
                        skip = True

                    if skip:
                        logger.trace(
                            f"Skipping incorrect episode torrent for {item.log_string}: {raw_title}"
                        )
                        continue

                if torrent.data.country and not item.is_anime:
                    # If country is present, then check to make sure it's correct. (Covers: US, UK, NZ, AU)
                    if (
                        torrent.data.country
                        and torrent.data.country not in _get_item_country(item)
                    ):
                        logger.trace(
                            f"Skipping torrent for incorrect country with {item.log_string}: {raw_title}"
                        )
                        continue

                if torrent.data.year and not _check_item_year(item, torrent.data):
                    # If year is present, then check to make sure it's correct
                    logger.debug(
                        f"Skipping torrent for incorrect year with {item.log_string}: {raw_title}"
                    )
                    continue

                if item.is_anime and scraping_settings.dubbed_anime_only:
                    # If anime and user wants dubbed only, then check to make sure it's dubbed
                    if not torrent.data.dubbed:
                        logger.trace(
                            f"Skipping non-dubbed anime torrent for {item.log_string}: {raw_title}"
                        )
                        continue

                torrents.add(torrent)
                processed_infohashes.add(infohash)
            except Exception as e:
                if log_msg:
                    logger.trace(f"GarbageTorrent: {e}")
                processed_infohashes.add(infohash)
                continue

        if torrents:
            logger.debug(
                f"Found {len(torrents)} streams for {item.log_string} using profile '{profile_name}'"
            )
            torrents = sort_torrents(
                torrents, bucket_limit=scraping_settings.bucket_limit
            )

            keep_versions = ranking_settings.get_keep_versions_for_profile(
                profile_name
            ) or 1

            added = 0
            for torrent in torrents.values():
                if added >= keep_versions:
                    break
                ih = torrent.infohash.lower()
                if ih in combined_streams:
                    duplicates.add(ih)
                    continue
                stream = Stream(torrent, profile_name=profile_name)
                combined_streams[ih] = stream
                added += 1

            logger.debug(
                f"Kept {added} streams for {item.log_string} after processing bucket limit with profile '{profile_name}'"
            )
        else:
            logger.debug(
                f"No valid torrents found for {item.log_string} using profile '{profile_name}' (duplicates seen: {len(duplicates)})"
            )

    if duplicates:
        logger.debug(
            f"Skipped {len(duplicates)} duplicate infohashes across profiles for {item.log_string}: {list(duplicates)[:5]}{'...' if len(duplicates) > 5 else ''}"
        )

    return combined_streams


# helper functions


def _check_item_year(item: MediaItem, data: ParsedData) -> bool:
    """Check if the year of the torrent is within the range of the item."""
    return data.year in [
        item.aired_at.year - 1,
        item.aired_at.year,
        item.aired_at.year + 1,
    ]


def _get_item_country(item: MediaItem) -> str:
    """Get the country code for a country."""
    country = ""

    if item.type == "season":
        country = item.parent.country.upper()
    elif item.type == "episode":
        country = item.parent.parent.country.upper()
    else:
        country = item.country.upper()

    # need to normalize
    if country == "USA":
        country = "US"
    elif country == "GB":
        country = "UK"

    return country


def select_top_n(
    streams: Dict[str, Stream], keep_versions: int
) -> Dict[str, Stream]:
    """
    Return a dictionary containing only the top N streams, preserving order.

    Args:
        streams: Ordered mapping of infohash -> Stream (already ranked)
        keep_versions: Number of streams to keep (minimum 1)

    Returns:
        Dict[str, Stream]: Trimmed mapping with at most N entries
    """
    if keep_versions <= 0:
        return {}
    items = list(streams.items())[:keep_versions]
    return {k: v for k, v in items}
