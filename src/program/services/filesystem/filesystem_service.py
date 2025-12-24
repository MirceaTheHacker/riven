"""Filesystem Service for Riven

This service provides a interface for filesystem operations
using the RivenVFS implementation.
"""

import os
from typing import Generator
from loguru import logger

from program.media.item import MediaItem
from program.settings.manager import settings_manager
from program.services.filesystem.common_utils import get_items_to_update
from program.services.downloaders import Downloader


class FilesystemService:
    """Filesystem service for VFS-only mode"""

    def __init__(self, downloader: Downloader):
        # Service key matches settings category name for reinitialization logic
        self.key = "filesystem"
        # Use filesystem settings
        self.settings = settings_manager.settings.filesystem
        self.riven_vfs = None
        self.downloader = downloader  # Store for potential reinit
        # Get symlink library path from environment variable (set by DUMB)
        # Check environment variable each time to allow it to be set after initialization
        self.symlink_library_path = os.getenv("RIVEN_SYMLINK_LIBRARY_PATH")
        if self.symlink_library_path:
            logger.info(f"FilesystemService: Symlink library path configured: {self.symlink_library_path}")
        else:
            logger.debug("FilesystemService: Symlink library path not configured (RIVEN_SYMLINK_LIBRARY_PATH not set)")
        self._initialize_rivenvfs(downloader)

    def _initialize_rivenvfs(self, downloader: Downloader):
        """Initialize or synchronize RivenVFS"""
        try:
            from .vfs import RivenVFS

            # If VFS already exists and is mounted, synchronize it with current settings
            if self.riven_vfs and getattr(self.riven_vfs, "_mounted", False):
                logger.info("Synchronizing existing RivenVFS with library profiles")
                self.riven_vfs.sync()
                return

            # Create new VFS instance
            logger.info("Initializing RivenVFS")
            self.riven_vfs = RivenVFS(
                mountpoint=str(self.settings.mount_path),
                downloader=downloader,
            )

        except ImportError as e:
            logger.error(f"Failed to import RivenVFS: {e}")
            logger.warning("RivenVFS initialization failed")
        except Exception as e:
            logger.error(f"Failed to initialize RivenVFS: {e}")
            logger.warning("RivenVFS initialization failed")

    def run(self, item: MediaItem) -> Generator[MediaItem, None, None]:
        """
        Process a MediaItem by registering its leaf media entries with the configured RivenVFS.

        Expands parent items (shows/seasons) into leaf items (episodes/movies), processes each leaf entry via add(), and yields the original input item for downstream state transitions. If RivenVFS is not available or there are no leaf items to process, the original item is yielded unchanged.

        Parameters:
            item (MediaItem): The media item (episode, movie, season, or show) to process.

        Returns:
            Generator[MediaItem, None, None]: Yields the original `item` once processing completes (or immediately if processing cannot proceed).
        """
        if not self.riven_vfs:
            logger.error("RivenVFS not initialized")
            yield item
            return

        # Expand parent items (show/season) to leaf items (episodes/movies)
        items_to_process = get_items_to_update(item)
        if not items_to_process:
            logger.debug(f"No items to process for {item.log_string}")
            yield item
            return

        # Process each episode/movie
        for episode_or_movie in items_to_process:
            # Re-check environment variable in case it was set after initialization
            symlink_path = self.symlink_library_path or os.getenv("RIVEN_SYMLINK_LIBRARY_PATH")
            if not self.symlink_library_path and symlink_path:
                self.symlink_library_path = symlink_path
                logger.info(f"FilesystemService: Symlink library path now configured: {self.symlink_library_path}")
            
            # Remove existing nodes to keep VFS in sync with current entries/retention
            # Also remove old symlinks before removing from VFS
            if self.symlink_library_path:
                self._remove_symlinks(episode_or_movie)
            self.riven_vfs.remove(episode_or_movie)
            success = self.riven_vfs.add(episode_or_movie)

            if not success:
                logger.error(f"Failed to register {item.log_string} with RivenVFS")
                continue

            logger.debug(f"Registered {episode_or_movie.log_string} with RivenVFS")
            
            # Create symlinks from VFS mount to symlink library path if configured
            if self.symlink_library_path:
                self._create_symlinks(episode_or_movie)

        logger.info(f"Filesystem processing complete for {item.log_string}")

        # Yield the original item for state transition
        yield item

    def close(self):
        """
        Close the underlying RivenVFS and release associated resources.

        If a RivenVFS instance is present, attempts to close it and always sets self.riven_vfs to None. Exceptions raised while closing are logged and not propagated.
        """
        try:
            if self.riven_vfs:
                self.riven_vfs.close()
        except Exception as e:
            logger.error(f"Error closing RivenVFS: {e}")
        finally:
            self.riven_vfs = None

    def validate(self) -> bool:
        """Validate service state and configuration.
        Checks that:
        - mount path is set
        - RivenVFS is initialized and mounted

        Note: Mount directory creation is handled by RivenVFS._prepare_mountpoint()
        """
        # Check mount path is set
        if not str(self.settings.mount_path):
            logger.error("FilesystemService: mount_path is empty")
            return False

        # Check RivenVFS is initialized
        if not self.riven_vfs:
            logger.error("FilesystemService: RivenVFS not initialized")
            return False

        # Check RivenVFS is mounted
        if not getattr(self.riven_vfs, "_mounted", False):
            logger.error("FilesystemService: RivenVFS not mounted")
            return False

        return True

    def _find_actual_file_path(self, entry, vfs_path: str) -> str | None:
        """
        Find the actual file path in /mnt/debrid/riven/ for a given MediaEntry.
        
        This searches for the file using the original_filename or infohash to match
        against files in /mnt/debrid/riven/movies/ or /mnt/debrid/riven/__all__/.
        
        Returns the actual file path if found, None otherwise.
        """
        original_filename = getattr(entry, "original_filename", None)
        infohash = getattr(entry, "infohash", None)
        
        if not original_filename:
            logger.debug(f"_find_actual_file_path: No original_filename for entry")
            return None
        
        logger.info(f"_find_actual_file_path: Searching for '{original_filename}' in /mnt/debrid/riven/")
        
        # Search in /mnt/debrid/riven/movies/ and /mnt/debrid/riven/__all__/
        search_dirs = ["/mnt/debrid/riven/movies", "/mnt/debrid/riven/__all__"]
        
        # Extract key parts from original filename for matching
        original_lower = original_filename.lower()
        # Get base filename without extension for matching
        base_name = os.path.splitext(original_filename)[0].lower()
        # Also try without the extension in the search
        filename_no_ext = os.path.splitext(os.path.basename(original_filename))[0].lower()
        
        for search_dir in search_dirs:
            if not os.path.exists(search_dir):
                logger.debug(f"_find_actual_file_path: Search directory does not exist: {search_dir}")
                continue
            
            try:
                # First, try using find command for faster searching (much faster than os.walk)
                import subprocess
                try:
                    # Escape the filename for shell safety and search
                    safe_pattern = original_filename.replace('[', '\\[').replace(']', '\\]')
                    find_cmd = ['find', search_dir, '-type', 'f', '-iname', f'*{safe_pattern}*', '-print', '-quit']
                    result = subprocess.run(find_cmd, capture_output=True, text=True, timeout=3)
                    
                    if result.returncode == 0 and result.stdout.strip():
                        file_path = result.stdout.strip()
                        if os.path.isfile(file_path) and os.access(file_path, os.R_OK):
                            logger.info(f"Found actual file via find command for {original_filename}: {file_path}")
                            return file_path
                except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
                    logger.debug(f"find command failed or not available, using os.walk: {e}")
                
                # Fallback to os.walk if find doesn't work
                files_checked = 0
                for root, dirs, files in os.walk(search_dir):
                    # Limit search depth to avoid going too deep
                    if root.count(os.sep) > search_dir.count(os.sep) + 2:
                        dirs[:] = []  # Don't recurse deeper
                        continue
                    
                    for file in files:
                        files_checked += 1
                        file_path = os.path.join(root, file)
                        file_lower = file.lower()
                        
                        # Match by original filename (case-insensitive)
                        # Check multiple matching strategies
                        file_basename = os.path.basename(file_path).lower()
                        matches = (
                            original_lower in file_lower or 
                            file_lower in original_lower or
                            base_name in file_lower or
                            filename_no_ext in file_basename or
                            file_basename == original_lower or
                            file_basename == base_name
                        )
                        
                        if matches:
                            # Verify it's a regular file and readable
                            if os.path.isfile(file_path) and os.access(file_path, os.R_OK):
                                logger.info(f"Found actual file for {original_filename}: {file_path}")
                                return file_path
                        
                        # Limit search to avoid being too slow (increased limit)
                        if files_checked > 2000:
                            logger.debug(f"_find_actual_file_path: Searched {files_checked} files in {search_dir}, stopping search to avoid timeout")
                            break
                    
                    if files_checked > 2000:
                        break
                
                logger.debug(f"_find_actual_file_path: Searched {files_checked} files in {search_dir}, no match found")
            except Exception as e:
                logger.warning(f"Error searching {search_dir} for {original_filename}: {e}")
                continue
        
        logger.debug(f"_find_actual_file_path: No actual file found for {original_filename}, will use VFS mount path")
        return None

    def _create_symlinks(self, item: MediaItem):
        """
        Create symlinks from actual files in /mnt/debrid/riven/ to symlink library path.
        
        This creates symlinks pointing to the actual downloaded files instead of the VFS mount,
        which ensures Plex can properly access and scan the files.
        """
        # Re-check environment variable in case it was set after initialization
        symlink_path = self.symlink_library_path or os.getenv("RIVEN_SYMLINK_LIBRARY_PATH")
        if not symlink_path:
            logger.debug(f"Symlink library path not configured, skipping symlink creation for {item.log_string}")
            return
        
        # Update instance variable if it was just read from environment
        if not self.symlink_library_path and symlink_path:
            self.symlink_library_path = symlink_path
            logger.info(f"FilesystemService: Symlink library path now configured: {self.symlink_library_path}")
        
        if not self.riven_vfs or not getattr(self.riven_vfs, "_mounted", False):
            logger.debug("RivenVFS not mounted, skipping symlink creation")
            return
        
        # Get all filesystem entries for this item
        entries = getattr(item, "filesystem_entries", None) or []
        if not entries:
            logger.debug(f"No filesystem entries for {item.log_string}, skipping symlink creation")
            return
        
        logger.debug(f"Creating symlinks for {item.log_string} with {len(entries)} filesystem entries, symlink path: {self.symlink_library_path}")
        
        mount_path = str(self.settings.mount_path)
        symlinks_created = 0
        
        for entry in entries:
            try:
                # Get all VFS paths for this entry
                vfs_paths = entry.get_all_vfs_paths()
                if not vfs_paths:
                    continue
                
                for vfs_path in vfs_paths:
                    # Try to find the actual file path in /mnt/debrid/riven/ first
                    # This ensures Plex can access the files properly (FUSE mounts through symlinks can be problematic)
                    actual_file_path = self._find_actual_file_path(entry, vfs_path)
                    
                    if actual_file_path:
                        # Use the actual file path instead of VFS mount
                        source_path = actual_file_path
                        logger.info(f"Using actual file path for symlink: {source_path} (instead of VFS mount)")
                    else:
                        # Fallback to VFS mount path
                        logger.warning(f"Could not find actual file path for {getattr(entry, 'original_filename', 'unknown')}, falling back to VFS mount - Plex may not be able to access this file")
                        source_path = os.path.join(mount_path, vfs_path.lstrip("/"))
                        
                        # Verify the source file actually exists in the VFS
                        # If not, try to find the actual file in the VFS directory
                        if not os.path.exists(source_path):
                            # Get the directory and filename from the path
                            vfs_dir = os.path.dirname(source_path)
                            expected_filename = os.path.basename(source_path)
                            
                            # If the directory exists, list files and try to find a match
                            if os.path.isdir(vfs_dir):
                                try:
                                    actual_files = os.listdir(vfs_dir)
                                    # Try to find a file that matches the entry's original filename
                                    # or matches the expected filename pattern
                                    matching_file = None
                                    entry_filename = getattr(entry, "original_filename", None) or ""
                                    
                                    for actual_file in actual_files:
                                        actual_path = os.path.join(vfs_dir, actual_file)
                                        # Match if it's the same base name or matches original filename
                                        if (os.path.basename(actual_file) == expected_filename or
                                            (entry_filename and entry_filename in actual_file)):
                                            matching_file = actual_file
                                            source_path = os.path.join(vfs_dir, matching_file)
                                            # Update vfs_path to match the actual file
                                            vfs_path = os.path.join(os.path.dirname(vfs_path), matching_file)
                                            logger.debug(f"Found actual VFS file: {matching_file} (expected: {expected_filename})")
                                            break
                                    
                                    if not matching_file:
                                        logger.warning(f"Source file does not exist and no match found: {source_path} (expected: {expected_filename}, found: {actual_files})")
                                        continue
                                except Exception as e:
                                    logger.warning(f"Failed to list VFS directory {vfs_dir}: {e}")
                                    continue
                            else:
                                logger.debug(f"VFS directory does not exist: {vfs_dir}, skipping symlink for {vfs_path}")
                                continue
                    
                    # Verify the source file exists and is readable
                    if not os.path.exists(source_path) or not os.access(source_path, os.R_OK):
                        logger.warning(f"Source file does not exist or is not readable: {source_path}, skipping symlink")
                        continue
                    
                    # Build target path (in symlink library)
                    target_path = os.path.join(self.symlink_library_path, vfs_path.lstrip("/"))
                    
                    # Create parent directories if needed
                    target_dir = os.path.dirname(target_path)
                    if target_dir and not os.path.exists(target_dir):
                        try:
                            os.makedirs(target_dir, exist_ok=True)
                        except Exception as e:
                            logger.warning(f"Failed to create directory {target_dir}: {e}")
                            continue
                    
                    # Create symlink if it doesn't exist or is broken
                    if os.path.exists(target_path):
                        if os.path.islink(target_path):
                            # Check if symlink is broken
                            link_target = os.readlink(target_path)
                            if not os.path.exists(link_target):
                                logger.debug(f"Removing broken symlink: {target_path} -> {link_target}")
                                try:
                                    os.remove(target_path)
                                except Exception as e:
                                    logger.warning(f"Failed to remove broken symlink {target_path}: {e}")
                                    continue
                            else:
                                # Symlink already exists and is valid
                                continue
                        else:
                            # Path exists but is not a symlink - skip to avoid overwriting
                            logger.debug(f"Path exists but is not a symlink: {target_path}, skipping")
                            continue
                    
                    # Create the symlink
                    try:
                        os.symlink(source_path, target_path)
                        symlinks_created += 1
                        logger.debug(f"Created symlink: {target_path} -> {source_path}")
                    except OSError as e:
                        logger.warning(f"Failed to create symlink {target_path} -> {source_path}: {e}")
            except Exception as e:
                logger.error(f"Error creating symlinks for {item.log_string}: {e}")
        
        if symlinks_created > 0:
            logger.info(f"Created {symlinks_created} symlink(s) for {item.log_string}")

    def _remove_symlinks(self, item: MediaItem):
        """
        Remove symlinks from symlink library path when item is removed from VFS.
        
        This cleans up symlinks when items are removed or updated.
        """
        if not self.symlink_library_path:
            return
        
        # Get all filesystem entries for this item
        entries = getattr(item, "filesystem_entries", None) or []
        if not entries:
            return
        
        symlinks_removed = 0
        
        for entry in entries:
            try:
                # Get all VFS paths for this entry
                vfs_paths = entry.get_all_vfs_paths()
                if not vfs_paths:
                    continue
                
                for vfs_path in vfs_paths:
                    # Build symlink path
                    symlink_path = os.path.join(self.symlink_library_path, vfs_path.lstrip("/"))
                    
                    # Remove symlink if it exists
                    if os.path.exists(symlink_path) or os.path.islink(symlink_path):
                        try:
                            if os.path.islink(symlink_path):
                                os.remove(symlink_path)
                                symlinks_removed += 1
                                logger.debug(f"Removed symlink: {symlink_path}")
                            
                            # Try to remove empty parent directories
                            parent_dir = os.path.dirname(symlink_path)
                            while parent_dir and parent_dir != self.symlink_library_path:
                                try:
                                    if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                                        os.rmdir(parent_dir)
                                        logger.debug(f"Removed empty directory: {parent_dir}")
                                        parent_dir = os.path.dirname(parent_dir)
                                    else:
                                        break
                                except OSError:
                                    break
                        except Exception as e:
                            logger.warning(f"Failed to remove symlink {symlink_path}: {e}")
            except Exception as e:
                logger.error(f"Error removing symlinks for {item.log_string}: {e}")
        
        if symlinks_removed > 0:
            logger.info(f"Removed {symlinks_removed} symlink(s) for {item.log_string}")

    @property
    def initialized(self) -> bool:
        """Check if the filesystem service is properly initialized"""
        return self.validate()
