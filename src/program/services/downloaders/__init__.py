from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from loguru import logger

from program.media.item import Episode, MediaItem, Movie, Show
from program.media.state import States
from program.media.stream import Stream
from program.media.media_entry import MediaEntry
from program.media.models import MediaMetadata
from program.settings.manager import settings_manager
from program.services.downloaders.models import (
    DebridFile,
    DownloadedTorrent,
    InvalidDebridFileException,
    NoMatchingFilesException,
    NotCachedException,
    TorrentContainer,
    TorrentInfo,
)
from RTN import ParsedData
from program.services.downloaders.shared import _sort_streams_by_quality, parse_filename
from program.utils.request import CircuitBreakerOpen

from .realdebrid import RealDebridDownloader
from .debridlink import DebridLinkDownloader
from .alldebrid import AllDebridDownloader


class Downloader:
    def __init__(self):
        self.key = "downloader"
        self.initialized = False
        self.services = {
            RealDebridDownloader: RealDebridDownloader(),
            DebridLinkDownloader: DebridLinkDownloader(),
            AllDebridDownloader: AllDebridDownloader(),
        }
        # Get all initialized services instead of just the first one
        self.initialized_services = [
            service for service in self.services.values() if service.initialized
        ]
        # Keep backward compatibility - primary service is the first initialized one
        self.service = (
            self.initialized_services[0] if self.initialized_services else None
        )
        self.initialized = self.validate()
        # Track circuit breaker retry attempts per item
        self._circuit_breaker_retries = {}
        # Track per-service cooldowns when circuit breaker is open
        self._service_cooldowns = {}  # {service.key: datetime}

    def validate(self):
        if not self.initialized_services:
            logger.error(
                "No downloader service is initialized. Please initialize a downloader service."
            )
            return False
        logger.info(
            f"Initialized {len(self.initialized_services)} downloader service(s): {', '.join(s.key for s in self.initialized_services)}"
        )
        return True

    def run(self, item: MediaItem):
        logger.debug(f"Starting download process for {item.log_string} ({item.id})")

        # Check if all services are in cooldown due to circuit breaker
        now = datetime.now()
        available_services = [
            service
            for service in self.initialized_services
            if service.key not in self._service_cooldowns
            or self._service_cooldowns[service.key] <= now
        ]

        if not available_services:
            # All services are in cooldown, reschedule for the earliest available time
            next_attempt = min(self._service_cooldowns.values())
            logger.warning(
                f"All downloader services in cooldown for {item.log_string} ({item.id}), rescheduling for {next_attempt.strftime('%m/%d/%y %H:%M:%S')}"
            )
            yield (item, next_attempt)
            return

        try:
            ranking_settings = settings_manager.settings.ranking
            library_path_setting = (
                settings_manager.settings.updaters.library_path
                or settings_manager.settings.filesystem.mount_path
            )
            library_path = str(library_path_setting) if library_path_setting else None
            default_profile_name = ranking_settings.get_profile_name_for_path(
                library_path
            )
            keep_versions = ranking_settings.get_keep_versions_for_profile(
                default_profile_name
            )
            # Ensure we attempt to keep at least as many streams as we currently have (multi-profile case)
            keep_versions = max(keep_versions or 1, len(item.streams) or 1)

            # Track if we hit circuit breaker on any service
            hit_circuit_breaker = False
            tried_streams = 0

            # Preserve scrape order and ensure we collect up to keep_versions distinct infohashes
            desired_hashes: list[str] = []
            desired_streams: list[Stream] = []
            for stream in item.streams:
                ih = stream.infohash.lower()
                if ih in desired_hashes:
                    continue
                desired_hashes.append(ih)
                desired_streams.append(stream)
                if len(desired_hashes) >= keep_versions:
                    break

            existing_infohashes = {
                getattr(entry, "infohash", "").lower()
                for entry in getattr(item, "filesystem_entries", [])
                if getattr(entry, "infohash", None)
            }

            # Ensure retention is enforced even if no new downloads occur (e.g., keep_versions decreased)
            self._enforce_version_retention(item, keep_versions, desired_hashes)
            existing_infohashes = {
                getattr(entry, "infohash", "").lower()
                for entry in getattr(item, "filesystem_entries", [])
                if getattr(entry, "infohash", None)
            }

            streams_to_process = [
                stream
                for stream in desired_streams
                if stream.infohash.lower() not in existing_infohashes
            ]

            download_success = False
            new_downloads = 0

            if not streams_to_process and len(existing_infohashes) >= keep_versions:
                download_success = True

            # For hq profile, validate top candidates and pick the largest one
            # This ensures we get the highest quality release when multiple options are available
            profile_name = getattr(streams_to_process[0], "profile_name", default_profile_name) if streams_to_process else default_profile_name
            validated_containers = {}
            unused_validated_torrents = []  # Track validated torrents we don't use, so we can clean them up
            
            if profile_name == "hq" and len(streams_to_process) > 1:
                # Validate top 5 candidates to get their sizes
                candidates_to_validate = streams_to_process[:5]
                validated_candidates = []
                
                # Get season information from W2P releases to detect single-season vs multi-season packs
                aliases_dict = getattr(item, "aliases", {}) or {}
                w2p_releases = aliases_dict.get("w2p_releases") or []
                season_map = {
                    rel.get("infohash", "").lower(): rel.get("season")
                    for rel in w2p_releases
                    if rel.get("infohash")
                }
                
                # Get target season if processing a specific season
                target_season = None
                if item.type == "season" and hasattr(item, "number"):
                    target_season = item.number
                
                for candidate_stream in candidates_to_validate:
                    for service in available_services:
                        try:
                            container = self.validate_stream_on_service(candidate_stream, item, service)
                            if container and container.torrent_info and container.torrent_info.bytes:
                                # Calculate median file size instead of total size
                                # This ensures multi-season bundles are compared fairly
                                median_file_size = self._calculate_median_file_size(container)
                                
                                # Check if this is a single-season release and if it matches target season
                                infohash_lower = candidate_stream.infohash.lower()
                                season = season_map.get(infohash_lower)
                                is_single_season = season is not None
                                matches_target_season = (target_season is not None and season == target_season) if season is not None else False
                                
                                validated_candidates.append({
                                    "stream": candidate_stream,
                                    "container": container,
                                    "service": service,
                                    "size_bytes": container.torrent_info.bytes,  # Keep total for logging
                                    "median_file_size": median_file_size,  # Use median for ranking
                                    "file_count": len(container.files) if container.files else 0,
                                    "torrent_id": container.torrent_id,
                                    "is_single_season": is_single_season,  # Track if single-season release
                                    "season": season,  # Season number if available
                                    "matches_target_season": matches_target_season,  # Track if matches target season
                                })
                                logger.debug(
                                    f"Validated candidate for {item.log_string}: {candidate_stream.infohash[:8]}... "
                                    f"total={container.torrent_info.size_mb:.2f}MB, "
                                    f"median_file={median_file_size / 1_000_000:.2f}MB, "
                                    f"files={len(container.files) if container.files else 0}, "
                                    f"season={'S' + str(season) if season is not None else 'pack/multi-season'}"
                                )
                                break  # Found on this service, move to next candidate
                        except Exception as e:
                            logger.debug(f"Failed to validate candidate {candidate_stream.infohash[:8]}...: {e}")
                            continue
                
                # Sort by: 1) matching target season first, 2) single-season releases, 3) median file size (largest first)
                # This ensures we prefer matching season releases with high quality over multi-season packs
                if validated_candidates:
                    validated_candidates.sort(key=lambda x: (-x["matches_target_season"], -x["is_single_season"], -x["median_file_size"]))
                    single_season_count = sum(1 for c in validated_candidates if c["is_single_season"])
                    pack_count = len(validated_candidates) - single_season_count
                    logger.info(
                        f"Re-ranked {len(validated_candidates)} candidates for {item.log_string} (hq profile): "
                        f"{single_season_count} single-season, {pack_count} packs/multi-season. "
                        f"Largest median: {validated_candidates[0]['median_file_size'] / 1_000_000:.2f}MB "
                        f"({validated_candidates[0]['file_count']} files, total={validated_candidates[0]['size_bytes'] / 1_000_000:.2f}MB, "
                        f"season={'S' + str(validated_candidates[0]['season']) if validated_candidates[0]['season'] is not None else 'pack'}), "
                        f"Smallest median: {validated_candidates[-1]['median_file_size'] / 1_000_000:.2f}MB "
                        f"({validated_candidates[-1]['file_count']} files, total={validated_candidates[-1]['size_bytes'] / 1_000_000:.2f}MB)"
                    )
                    # Replace streams_to_process with size-sorted candidates
                    streams_to_process = [c["stream"] for c in validated_candidates] + streams_to_process[len(candidates_to_validate):]
                    # Store validated containers for later use
                    for c in validated_candidates:
                        stream_key = c["stream"].infohash.lower()
                        validated_containers[stream_key] = (c["container"], c["service"])
                        # Track all validated torrents for potential cleanup (we'll remove the one we use)
                        if c["torrent_id"]:
                            unused_validated_torrents.append((c["service"], c["torrent_id"], stream_key))

            for stream in streams_to_process:
                # Stop once we have keep_versions worth of distinct infohashes
                if len(existing_infohashes) >= keep_versions:
                    break

                # Try each available service for this stream before blacklisting
                stream_failed_on_all_services = True
                stream_hit_circuit_breaker = False

                for service in available_services:
                    logger.debug(
                        f"Trying stream {stream.infohash} on {service.key} for {item.log_string}"
                    )

                    try:
                        # Use pre-validated container if available (for hq profile size-based ranking)
                        stream_key = stream.infohash.lower()
                        container = None
                        if stream_key in validated_containers:
                            container, validated_service = validated_containers[stream_key]
                            # Use the service that validated it
                            service = validated_service
                            median_size = self._calculate_median_file_size(container)
                            file_count = len(container.files) if container.files else 0
                            logger.debug(
                                f"Using pre-validated container for {stream.infohash[:8]}... "
                                f"(median_file={median_size / 1_000_000:.2f}MB, "
                                f"files={file_count}, total={container.torrent_info.size_mb:.2f}MB)"
                            )
                        else:
                            # Validate stream on this specific service
                            container: Optional[TorrentContainer] = (
                                self.validate_stream_on_service(stream, item, service)
                            )
                            if not container:
                                logger.debug(
                                    f"Stream {stream.infohash} not available on {service.key}"
                                )
                                continue

                        # Try to download using this service
                        try:
                            download_result = self.download_cached_stream_on_service(
                                stream, container, service
                            )
                        except Exception as download_error:
                            # If download fails and we used a pre-validated container, 
                            # the torrent might have been deleted - re-validate
                            if stream_key in validated_containers:
                                logger.debug(
                                    f"Pre-validated container failed for {stream.infohash[:8]}..., re-validating: {download_error}"
                                )
                                # Remove from validated containers and re-validate
                                del validated_containers[stream_key]
                                container = self.validate_stream_on_service(stream, item, service)
                                if not container:
                                    logger.debug(
                                        f"Stream {stream.infohash} not available on {service.key} after re-validation"
                                    )
                                    continue
                                download_result = self.download_cached_stream_on_service(
                                    stream, container, service
                                )
                            else:
                                raise
                        stream_profile = getattr(
                            stream, "profile_name", default_profile_name
                        )
                        if self.update_item_attributes(
                            item,
                            download_result,
                            service,
                            keep_versions=keep_versions,
                            profile_name=stream_profile,
                        ):
                            logger.log(
                                "DEBRID",
                                f"Downloaded {item.log_string} from '{stream.raw_title}' [{stream.infohash}] using {service.key}",
                            )
                            download_success = True
                            stream_failed_on_all_services = False
                            existing_infohashes.add(stream.infohash.lower())
                            new_downloads += 1
                            
                            # Remove the torrent we successfully downloaded from cleanup list
                            # (we'll clean up the rest at the end after processing all streams)
                            if unused_validated_torrents:
                                unused_validated_torrents = [
                                    (s, tid, key) for s, tid, key in unused_validated_torrents
                                    if key != stream_key
                                ]
                            
                            break
                        else:
                            raise NoMatchingFilesException(
                                f"No valid files found for {item.log_string} ({item.id})"
                            )

                    except CircuitBreakerOpen as e:
                        # This specific service hit circuit breaker, set cooldown and try next service
                        cooldown_duration = timedelta(minutes=1)
                        self._service_cooldowns[service.key] = (
                            datetime.now() + cooldown_duration
                        )
                        logger.warning(
                            f"Circuit breaker OPEN for {service.key}, trying next service for stream {stream.infohash}"
                        )
                        stream_hit_circuit_breaker = True
                        hit_circuit_breaker = True

                        # If this is the only initialized service, don't mark stream as failed
                        # We want to retry this stream after cooldown
                        if len(self.initialized_services) == 1:
                            stream_failed_on_all_services = False
                        continue

                    except Exception as e:
                        logger.debug(
                            f"Stream {stream.infohash} failed on {service.key}: {e}"
                        )
                        if "download_result" in locals() and download_result.id:
                            try:
                                service.delete_torrent(download_result.id)
                                logger.debug(
                                    f"Deleted failed torrent {stream.infohash} for {item.log_string} ({item.id}) on {service.key}."
                                )
                            except Exception as del_e:
                                logger.debug(
                                    f"Failed to delete torrent {stream.infohash} for {item.log_string} ({item.id}) on {service.key}: {del_e}"
                                )
                        continue

                # Only blacklist if stream genuinely failed on ALL available services
                # Don't blacklist if we hit circuit breaker in single-provider mode
                if stream_failed_on_all_services:
                    if (
                        stream_hit_circuit_breaker
                        and len(self.initialized_services) == 1
                    ):
                        logger.debug(
                            f"Stream {stream.infohash} hit circuit breaker on single provider, will retry after cooldown"
                        )
                    else:
                        logger.debug(
                            f"Stream {stream.infohash} failed on all {len(available_services)} available service(s), blacklisting"
                        )
                        item.blacklist_stream(stream)

                tried_streams += 1
                if tried_streams >= 3:
                    yield item

            # Clean up any remaining unused validated torrents after processing all streams
            # This ensures we don't delete torrents that might be needed for other profiles (e.g., mobile)
            if unused_validated_torrents:
                logger.debug(f"Cleaning up {len(unused_validated_torrents)} unused validated torrents for {item.log_string} after processing all streams")
                for unused_service, unused_torrent_id, _ in unused_validated_torrents:
                    try:
                        unused_service.delete_torrent(unused_torrent_id)
                        logger.debug(f"Deleted unused validated torrent {unused_torrent_id} from {unused_service.key}")
                    except Exception as e:
                        logger.debug(f"Failed to delete unused validated torrent {unused_torrent_id}: {e}")

            # Final retention check after processing downloads
            self._enforce_version_retention(item, keep_versions, desired_hashes)

        except Exception as e:
            logger.error(
                f"Unexpected error in downloader for {item.log_string} ({item.id}): {e}"
            )

        if not download_success:
            # Check if we hit circuit breaker in single-provider mode
            if hit_circuit_breaker and len(self.initialized_services) == 1:
                # Reschedule for after cooldown instead of failing
                next_attempt = min(self._service_cooldowns.values())
                logger.warning(
                    f"Single provider hit circuit breaker for {item.log_string} ({item.id}), rescheduling for {next_attempt.strftime('%m/%d/%y %H:%M:%S')}"
                )
                yield (item, next_attempt)
                return
            else:
                logger.debug(
                    f"Failed to download any streams for {item.log_string} ({item.id})"
                )
        else:
            # Clear retry count and service cooldowns on successful download
            self._circuit_breaker_retries.pop(item.id, None)
            self._service_cooldowns.clear()

        yield item

    def _enforce_version_retention(
        self,
        item: MediaItem,
        keep_versions: int,
        desired_hashes: Optional[list[str]] = None,
    ) -> None:
        """
        Ensure only the top N filesystem entries are retained per profile.
        
        This allows multiple entries with the same infohash if they have different profiles
        (e.g., one for 'mobile' and one for 'hq'), but limits entries per profile to keep_versions.
        """
        keep_versions = keep_versions if keep_versions and keep_versions > 0 else 1
        entries = list(getattr(item, "filesystem_entries", []))
        if not entries:
            return

        # Group entries by profile_name, then by infohash within each profile
        from collections import defaultdict
        entries_by_profile: dict[str | None, dict[str, list[MediaEntry]]] = defaultdict(lambda: defaultdict(list))
        
        for entry in entries:
            # Extract profile_name from media_metadata
            profile_name = None
            if hasattr(entry, "media_metadata") and entry.media_metadata:
                if isinstance(entry.media_metadata, dict):
                    profile_name = entry.media_metadata.get("profile_name")
                else:
                    profile_name = getattr(entry.media_metadata, "profile_name", None)
            
            ih = getattr(entry, "infohash", "").lower()
            if ih:
                entries_by_profile[profile_name][ih].append(entry)
            else:
                # Entries without infohash go into a special group
                entries_by_profile[profile_name][""].append(entry)

        if desired_hashes:
            ordered_hashes = [h.lower() for h in desired_hashes]
        else:
            ordered_hashes = []
            for stream in _sort_streams_by_quality(item.streams):
                ih = stream.infohash.lower()
                if ih not in ordered_hashes:
                    ordered_hashes.append(ih)

        keep_list: list[MediaEntry] = []
        
        # For each profile, keep up to keep_versions entries
        for profile_name, profile_entries in entries_by_profile.items():
            profile_keep_list: list[MediaEntry] = []
            
            # First, add entries for desired_hashes in order
            for ih in ordered_hashes:
                if ih in profile_entries:
                    for entry in profile_entries[ih]:
                        if entry not in profile_keep_list:
                            profile_keep_list.append(entry)
                        if len(profile_keep_list) >= keep_versions:
                            break
                if len(profile_keep_list) >= keep_versions:
                    break
            
            # Fill remaining slots for this profile with any other entries
            if len(profile_keep_list) < keep_versions:
                for ih, entry_list in profile_entries.items():
                    if ih not in ordered_hashes:  # Skip already processed hashes
                        for entry in entry_list:
                            if entry not in profile_keep_list:
                                profile_keep_list.append(entry)
                            if len(profile_keep_list) >= keep_versions:
                                break
                    if len(profile_keep_list) >= keep_versions:
                        break
            
            # Add all kept entries for this profile to the main keep_list
            keep_list.extend(profile_keep_list)

        # Reorder/trim to keep list only
        item.filesystem_entries.clear()
        item.filesystem_entries.extend(keep_list)

        # Keep active_stream aligned with best retained entry when possible
        if keep_list:
            top_infohash = getattr(keep_list[0], "infohash", None)
            if top_infohash:
                item.active_stream = {
                    "infohash": top_infohash,
                    "id": item.active_stream.get("id") if item.active_stream else None,
                }

    def validate_stream(
        self, stream: Stream, item: MediaItem
    ) -> Optional[TorrentContainer]:
        """
        Validate a single stream by ensuring its files match the item's requirements.
        Uses the primary service for backward compatibility.
        """
        return self.validate_stream_on_service(stream, item, self.service)

    def _calculate_median_file_size(self, container: TorrentContainer) -> int:
        """
        Calculate the median file size from a torrent container.
        
        This is used for fair comparison between multi-season bundles and single-season bundles.
        A bundle with 9 seasons might have a huge total size, but if each episode is low quality,
        the median will be low, making it rank lower than a single-season bundle with high-quality episodes.
        
        Args:
            container: TorrentContainer with files
            
        Returns:
            Median file size in bytes, or 0 if no files or sizes available
        """
        if not container or not container.files:
            return 0
        
        # Get file sizes, filtering out None values
        file_sizes = [
            file.filesize for file in container.files
            if file.filesize is not None and file.filesize > 0
        ]
        
        if not file_sizes:
            # Fallback to total torrent size if individual file sizes aren't available
            if container.torrent_info and container.torrent_info.bytes:
                return container.torrent_info.bytes
            return 0
        
        # Calculate median
        file_sizes.sort()
        n = len(file_sizes)
        if n == 0:
            return 0
        elif n % 2 == 0:
            # Even number of files: average of two middle values
            median = (file_sizes[n // 2 - 1] + file_sizes[n // 2]) / 2
        else:
            # Odd number of files: middle value
            median = file_sizes[n // 2]
        
        return int(median)

    def validate_stream_on_service(
        self, stream: Stream, item: MediaItem, service
    ) -> Optional[TorrentContainer]:
        """
        Validate a single stream on a specific service by ensuring its files match the item's requirements.
        """
        container = service.get_instant_availability(stream.infohash, item.type)
        if not container:
            logger.debug(
                f"Stream {stream.infohash} is not cached or valid on {service.key}."
            )
            return None

        valid_files = []
        for file in container.files or []:
            if isinstance(file, DebridFile):
                valid_files.append(file)
                continue

            try:
                debrid_file = DebridFile.create(
                    filename=file.filename,
                    filesize_bytes=file.filesize,
                    filetype=item.type,
                    file_id=file.file_id,
                )

                if isinstance(debrid_file, DebridFile):
                    valid_files.append(debrid_file)
            except InvalidDebridFileException as e:
                logger.debug(f"{stream.infohash}: {e}")
                continue

        if valid_files:
            container.files = valid_files
            return container

        return None

    def update_item_attributes(
        self,
        item: MediaItem,
        download_result: DownloadedTorrent,
        service=None,
        keep_versions: int = 1,
        profile_name: str | None = None,
    ) -> bool:
        """Update the item attributes with the downloaded files and active stream."""
        if service is None:
            service = self.service

        try:
            if not download_result.container:
                raise NotCachedException(
                    f"No container found for {item.log_string} ({item.id})"
                )

            episode_cap: int = None
            show: Optional[Show] = None
            if item.type in ("show", "season", "episode"):
                show = (
                    item
                    if item.type == "show"
                    else (item.parent if item.type == "season" else item.parent.parent)
                )
                try:
                    method_1 = sum(len(season.episodes) for season in show.seasons)
                    try:
                        method_2 = show.seasons[-1].episodes[-1].number
                    except IndexError:
                        # happens if theres a new season with no episodes yet
                        method_2 = show.seasons[-2].episodes[-1].number
                    episode_cap = max([method_1, method_2])
                except Exception as e:
                    pass
            found = False
            files = list(download_result.container.files or [])
            # Track episodes we've already processed to avoid duplicates
            processed_episode_ids: set[str] = set()

            for file in files:
                try:
                    file_data: ParsedData = parse_filename(file.filename)
                except Exception as e:
                    continue

                if item.type in ("show", "season", "episode"):
                    if not file_data.episodes:
                        continue
                    elif 0 in file_data.episodes and len(file_data.episodes) == 1:
                        continue
                    elif file_data.seasons and file_data.seasons[0] == 0:
                        continue

                if self.match_file_to_item(
                    item,
                    file_data,
                    file,
                    download_result,
                    show,
                    episode_cap,
                    processed_episode_ids,
                    service,
                    keep_versions,
                    profile_name,
                ):
                    found = True

            return found
        except Exception as e:
            logger.debug(f"update_item_attributes: exception for item {item.id}: {e}")
            raise

    def match_file_to_item(
        self,
        item: MediaItem,
        file_data: ParsedData,
        file: DebridFile,
        download_result: DownloadedTorrent,
        show: Optional[Show] = None,
        episode_cap: int = None,
        processed_episode_ids: Optional[set[str]] = None,
        service=None,
        keep_versions: int = 1,
        profile_name: str | None = None,
    ) -> bool:
        """
        Determine whether a parsed file corresponds to the given media item (movie, show, season, or episode) and update the item's attributes when matches are found.

        Checks movie matches for movie items and episode-level matches for shows/seasons/episodes. For each matched episode or movie file, calls _update_attributes to attach filesystem metadata and marks the item.active_stream when appropriate.

        Parameters:
            item (MediaItem): The target media item to match against.
            file_data (ParsedData): Parsed metadata from RTN (item type, season, episode list, etc.).
            file (DebridFile): The debrid file candidate containing filename, download URL, and size.
            download_result (DownloadedTorrent): The download context containing infohash and torrent id.
            show (Optional[Show]): The show object used to resolve absolute episode numbers when matching episodes.
            episode_cap (int, optional): Maximum episode number allowed for matching; episodes greater than this are skipped.
            processed_episode_ids (Optional[set[str]]): Set of episode IDs already processed in this container to avoid duplicate updates.
            service (optional): Service instance used for attribute updates; defaults to the Downloader's primary service.

        Returns:
            bool: `true` if at least one file-to-item match was found and attributes were updated, `false` otherwise.
        """
        if service is None:
            service = self.service

        logger.debug(
            f"match_file_to_item: item={item.id} type={item.type} file='{file.filename}'"
        )
        found = False

        if item.type == "movie" and file_data.type == "movie":
            logger.debug("match_file_to_item: movie match -> updating attributes")
            self._update_attributes(
                item,
                file,
                download_result,
                service,
                file_data,
                keep_versions,
                profile_name,
            )
            return True

        if item.type in ("show", "season", "episode"):
            season_number = file_data.seasons[0] if file_data.seasons else None
            for file_episode in file_data.episodes:
                if episode_cap and file_episode > episode_cap:
                    logger.debug(
                        f"Invalid episode number {file_episode} for {getattr(show, 'log_string', 'show?')}. Skipping '{file.filename}'"
                    )
                    continue

                episode: Episode = show.get_absolute_episode(
                    file_episode, season_number
                )
                if episode is None:
                    logger.debug(
                        f"Episode {file_episode} from file does not match any episode in {getattr(show, 'log_string', 'show?')}"
                    )
                    continue

                # Allow processing episodes in Downloaded state to support multiple versions (hq, mobile)
                # Only skip episodes that are in final states (Completed, Symlinked)
                if episode and episode.state not in [
                    States.Completed,
                    States.Symlinked,
                ]:
                    # Skip if we've already processed this episode in this container
                    if (
                        processed_episode_ids is not None
                        and str(episode.id) in processed_episode_ids
                    ):
                        continue
                    logger.debug(
                        f"match_file_to_item: updating episode {episode.id} from file '{file.filename}'"
                    )
                    self._update_attributes(
                        episode,
                        file,
                        download_result,
                        service,
                        file_data,
                        keep_versions,
                        profile_name,
                    )
                    if processed_episode_ids is not None:
                        processed_episode_ids.add(str(episode.id))
                    logger.debug(
                        f"Matched episode {episode.log_string} to file {file.filename}"
                    )
                    found = True

        if found and item.type in ("show", "season"):
            item.active_stream = {
                "infohash": download_result.infohash,
                "id": download_result.info.id,
            }

        return found

    def download_cached_stream(
        self, stream: Stream, container: TorrentContainer
    ) -> DownloadedTorrent:
        """Download a cached stream using the primary service"""
        return self.download_cached_stream_on_service(stream, container, self.service)

    def download_cached_stream_on_service(
        self, stream: Stream, container: TorrentContainer, service
    ) -> DownloadedTorrent:
        """
        Prepare and return a DownloadedTorrent for a stream using the given service.

        Uses values already present on `container` when available (e.g., `torrent_id`, `torrent_info`); otherwise adds the torrent and/or fetches its info from the service.

        Returns:
            DownloadedTorrent: An object containing the torrent id, torrent info, the stream's infohash, and the (possibly updated) container.
        """
        # Check if we already have a torrent_id from validation (Real-Debrid optimization)
        if container.torrent_id:
            torrent_id = container.torrent_id
            logger.debug(
                f"Reusing torrent_id {torrent_id} from validation for {stream.infohash}"
            )

        # Check if we already have torrent_info from validation (Real-Debrid optimization)
        if container.torrent_info:
            info = container.torrent_info
            logger.debug(f"Reusing cached torrent_info for {stream.infohash}")
        else:
            # Fallback: fetch info if not cached
            info: TorrentInfo = service.get_torrent_info(torrent_id)

        if container.file_ids:
            service.select_files(torrent_id, container.file_ids)

        return DownloadedTorrent(
            id=torrent_id, info=info, infohash=stream.infohash, container=container
        )

    def _update_attributes(
        self,
        item: Union[Movie, Episode],
        debrid_file: DebridFile,
        download_result: DownloadedTorrent,
        service=None,
        file_data: ParsedData = None,
        keep_versions: int = 1,
        profile_name: str | None = None,
    ) -> None:
        """
        Update the media item's active stream and filesystem entries using a debrid file from a completed download.

        Sets item.active_stream from the download_result and, if the debrid file exposes a download URL,
        creates a MediaEntry with the original filename, download URL, and provider information.
        Path generation is now handled by RivenVFS when the entry is registered.

        Parameters:
            item (Movie|Episode): The media item to update.
            debrid_file (DebridFile): Debrid file metadata (must include filename and optionally download_url and filesize).
            download_result (DownloadedTorrent): Result of the download containing id and infohash.
            service: Optional debrid service instance; defaults to the downloader's configured service.
            file_data (ParsedData, optional): Parsed filename metadata from RTN to cache in MediaEntry.
        """
        if service is None:
            service = self.service

        item.active_stream = {
            "infohash": download_result.infohash,
            "id": download_result.info.id,
        }

        # Create MediaEntry for virtual file if download URL is available
        if debrid_file.download_url:
            from program.services.library_profile_matcher import LibraryProfileMatcher

            # Match library profiles for this item (for path generation)
            matcher = LibraryProfileMatcher()
            library_profiles = matcher.get_matching_profiles(item)
            logger.debug(
                f"Library profile matching for {item.log_string}: found {len(library_profiles)} profiles: {library_profiles}"
            )

            # Only create entries for the profile that actually matched this torrent
            # This ensures we don't create duplicate entries pointing to the same file
            # If you want multiple versions (hq and mobile), you need to find separate torrents for each profile
            ranking_profiles_to_create = []
            if profile_name:
                ranking_profiles_to_create = [profile_name]
                logger.info(
                    f"Creating MediaEntry for profile '{profile_name}' that matched this torrent for {item.log_string}"
                )
            else:
                ranking_profiles_to_create = [None]
                logger.info(
                    f"No profile_name from stream, creating entry without profile for {item.log_string}"
                )

            # Create MediaEntry with original_filename as source of truth
            # Path generation is now handled by RivenVFS during registration
            # Convert parsed file_data to MediaMetadata if available
            media_metadata = None
            if file_data:
                metadata = MediaMetadata.from_parsed_data(
                    file_data.model_dump(), filename=debrid_file.filename
                )
                media_metadata = metadata.model_dump(mode="json")

            # Determine which profiles need entries
            # Use ranking profiles (hq, mobile) instead of library_profiles
            profiles_to_create = ranking_profiles_to_create
            logger.debug(
                f"Creating entries for {len(profiles_to_create)} ranking profiles: {profiles_to_create} for {item.log_string}"
            )

            # Create or update entries for each profile
            for target_profile in profiles_to_create:
                # Create a copy of the entry metadata with the target profile
                entry_metadata = media_metadata.copy() if media_metadata else {}
                if target_profile:
                    entry_metadata["profile_name"] = target_profile
                elif "profile_name" in entry_metadata:
                    # Remove profile_name if target_profile is None
                    entry_metadata.pop("profile_name", None)

                # Create entry with this profile
                profile_entry = MediaEntry.create_virtual_entry(
                    original_filename=debrid_file.filename,
                    download_url=debrid_file.download_url,
                    provider=service.key,
                    provider_download_id=str(download_result.info.id),
                    file_size=debrid_file.filesize or 0,
                    media_metadata=entry_metadata,
                    infohash=download_result.infohash.lower(),
                )
                profile_entry.library_profiles = library_profiles

                # Check if an entry with the same infohash AND profile_name already exists
                # We need to check ALL entries, not just the first match, to ensure we don't
                # accidentally update an entry with a different profile_name
                existing_entry = None
                for e in item.filesystem_entries:
                    if getattr(e, "infohash", "").lower() == download_result.infohash.lower():
                        # Check if profile_name matches
                        existing_profile = None
                        if hasattr(e, "media_metadata") and e.media_metadata:
                            existing_profile = e.media_metadata.get("profile_name") if isinstance(e.media_metadata, dict) else getattr(e.media_metadata, "profile_name", None)
                        
                        # If both have the same profile (or both are None), update in place
                        # Use explicit None comparison to handle both None and empty string cases
                        if (existing_profile is None and target_profile is None) or (existing_profile == target_profile):
                            existing_entry = e
                            break
                
                if existing_entry:
                    # Update existing entry
                    existing_entry.original_filename = profile_entry.original_filename
                    existing_entry.download_url = profile_entry.download_url
                    existing_entry.unrestricted_url = profile_entry.unrestricted_url
                    existing_entry.provider = profile_entry.provider
                    existing_entry.provider_download_id = profile_entry.provider_download_id
                    existing_entry.file_size = profile_entry.file_size
                    existing_entry.media_metadata = profile_entry.media_metadata
                    existing_entry.library_profiles = profile_entry.library_profiles
                    existing_entry.infohash = profile_entry.infohash
                    logger.debug(
                        f"Updated MediaEntry for {item.log_string} with profile_name={target_profile}"
                    )
                else:
                    # Create new entry
                    item.filesystem_entries.append(profile_entry)
                    logger.debug(
                        f"Created MediaEntry for {item.log_string} with profile_name={target_profile}"
                    )

            # Enforce retention after adding/updating
            self._enforce_version_retention(item, keep_versions)

            logger.debug(
                f"Created MediaEntry for {item.log_string} with original_filename={debrid_file.filename}"
            )
            if library_profiles:
                logger.debug(
                    f"Matched library profiles for {item.log_string}: {library_profiles}"
                )

    def get_instant_availability(
        self, infohash: str, item_type: str
    ) -> List[TorrentContainer]:
        """
        Retrieve cached availability information for a torrent identified by its infohash and item type.

        Queries the active downloader service for instant availability and returns any matching cached torrent containers.

        Returns:
            List[TorrentContainer]: A list of TorrentContainer objects representing available cached torrents; empty list if none are found.
        """
        return self.service.get_instant_availability(infohash, item_type)

    def add_torrent(self, infohash: str) -> int:
        """Add a torrent by infohash"""
        return self.service.add_torrent(infohash)

    def get_torrent_info(self, torrent_id: int) -> TorrentInfo:
        """Get information about a torrent"""
        return self.service.get_torrent_info(torrent_id)

    def select_files(self, torrent_id: int, container: list[str]) -> None:
        """Select files from a torrent"""
        self.service.select_files(torrent_id, container)

    def delete_torrent(self, torrent_id: int) -> None:
        """Delete a torrent"""
        self.service.delete_torrent(torrent_id)

    def get_user_info(self, service) -> Dict:
        """Get user information"""
        return service.get_user_info()
