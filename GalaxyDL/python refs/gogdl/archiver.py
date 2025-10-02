#!/usr/bin/env python3
"""
GOG Galaxy Archiver - Similar to SteamArchiver_Python but for GOG Galaxy CDN
Collects and archives manifests, chunks, and metadata from GOG Galaxy CDN
"""

import os
import json
import zlib
import gzip
import hashlib
import time
import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

import gogdl.api as api
import gogdl.auth as auth
from gogdl.dl import dl_utils
from gogdl import constants
from gogdl.dl.objects import v1, v2


@dataclass
class ArchivedChunk:
    """Represents an archived chunk/blob from v2 manifests"""
    md5: str
    sha256: Optional[str]
    compressed_size: int
    archive_path: str
    cdn_path: str
    first_seen: float
    last_verified: float


@dataclass 
class ArchivedBlob:
    """Represents an archived binary blob from v1 manifests (main.bin files)"""
    depot_manifest: str  # The blob URL identifier (e.g. "1207658930/main.bin")
    secure_url: str      # The secure CDN URL base
    total_size: int
    archive_path: str
    first_seen: float
    last_verified: float
    files_contained: List[Dict] = None  # Optional - use manifests instead for file info
    depot_info: Dict = None  # Contains referencing manifests and metadata
    depot_info: Dict     # Store depot metadata


@dataclass
class ArchivedManifest:
    """Represents an archived depot manifest (v1 or v2)"""
    manifest_id: str
    game_id: str
    version: int  # 1 or 2
    manifest_type: str  # 'depot', 'offline_depot', etc.
    languages: List[str]
    archive_path: str  # Relative path to raw manifest file
    cdn_url: str
    timestamp: float
    file_count: int
    total_size: int  # Total uncompressed size of all files
    chunks_referenced: Set[str]  # For v2: chunk MD5s, For v1: file hashes
    
    
@dataclass
class ArchivedBuild:
    """Represents an archived build manifest"""
    game_id: str
    build_id: str
    build_hash: str
    platform: str
    version: int  # 1 or 2
    archive_path: str
    cdn_url: str
    timestamp: float
    dependencies: List[str]
    manifests_referenced: Set[str]
    repository_id: str = None  # Optional field for repository/manifest ID
    version_name: str = ""  # Game version string (e.g., "3.5.0.26g")
    tags: List[str] = None  # Build tags (e.g., ["receiver_v1", "csb_10_6_1_w_158"])
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


class GOGGalaxyArchiver:
    """
    GOG Galaxy CDN Archiver - Archives manifests, chunks, and metadata
    """
    
    def __init__(self, archive_root: str, auth_config_path: str = None):
        self.archive_root = Path(archive_root)
        
        # Raw CDN data storage (mirrors CDN structure)
        self.builds_dir = self.archive_root / "builds"        # Raw build manifests
        self.manifests_dir = self.archive_root / "manifests"  # Raw depot manifests  
        self.chunks_dir = self.archive_root / "chunks"        # v2 chunks (MD5-named)
        self.blobs_dir = self.archive_root / "blobs"          # v1 binary blobs 
        
        # Our processed data and indexes
        self.metadata_dir = self.archive_root / "metadata"    # Our database/indexes only
        self.database_path = self.metadata_dir / "archive_database.json"
        
        # Create directories
        for dir_path in [self.builds_dir, self.manifests_dir, self.chunks_dir, self.blobs_dir, self.metadata_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        # Initialize API (optional for validation)
        if auth_config_path is not None:
            self.auth_manager = auth.AuthorizationManager(auth_config_path)
            self.api_handler = api.ApiHandler(self.auth_manager)
        else:
            self.auth_manager = None
            self.api_handler = None
        
        # Initialize logger
        self.logger = logging.getLogger("GOGGalaxyArchiver")
        
        # Archive database - streamlined to only track builds
        # Chunks, blobs, and manifests verified from file system
        self.archived_builds: Dict[str, ArchivedBuild] = {}
        
        # TEMPORARY: Keep in-memory tracking for compatibility but don't save to database
        self.archived_chunks: Dict[str, ArchivedChunk] = {}  # In-memory only
        self.archived_blobs: Dict[str, ArchivedBlob] = {}    # In-memory only  
        self.archived_manifests: Dict[str, ArchivedManifest] = {}  # In-memory only
        
        self.load_database()
        
    def save_raw_build_manifest(self, cdn_url: str, raw_data: bytes, version: int) -> str:
        """Save raw build manifest data exactly as received from CDN"""
        # Use the same logic as _save_raw_build_manifest for consistent directory structure
        if '/v1/' in cdn_url:
            version_str = 'v1'
            # v1 URLs are different: /v1/manifests/1207658930/windows/37794096/repository.json
            url_parts = cdn_url.split('/v1/manifests/')
            if len(url_parts) > 1:
                path_part = url_parts[1]  # "1207658930/windows/37794096/repository.json"
                raw_path = self.builds_dir / version_str / "manifests" / path_part
            else:
                # Fallback
                filename = cdn_url.split('/')[-1]
                raw_path = self.builds_dir / version_str / filename
        elif '/v2/' in cdn_url:
            version_str = 'v2'
            # v2 URLs: /content-system/v2/meta/92/ab/92ab42631ff4742b309bb62c175e6306
            url_parts = cdn_url.split('/v2/')
            if len(url_parts) > 1:
                path_part = url_parts[1]  # "meta/92/ab/92ab42631ff4742b309bb62c175e6306"
                raw_path = self.builds_dir / version_str / path_part
            else:
                # Fallback
                filename = cdn_url.split('/')[-1]
                raw_path = self.builds_dir / version_str / filename
        else:
            # Unknown version, save in root
            filename = cdn_url.split('/')[-1]
            raw_path = self.builds_dir / filename

        raw_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save exactly as received (compressed)
        with open(raw_path, 'wb') as f:
            f.write(raw_data)
            
        self.logger.debug(f"Saved raw build manifest: {raw_path} ({len(raw_data)} bytes)")
        return str(raw_path)
        
    def load_database(self):
        """Load existing archive database - streamlined to only track builds"""
        if self.database_path.exists():
            try:
                with open(self.database_path, 'r') as f:
                    data = json.load(f)
                    
                # Load builds only - all other data comes from file system
                for build_data in data.get('builds', []):
                    # Handle field name changes for backwards compatibility
                    if 'chunks_referenced' in build_data:
                        build_data['manifests_referenced'] = build_data.pop('chunks_referenced')
                    if 'manifest_hash' in build_data:
                        build_data['build_hash'] = build_data.pop('manifest_hash')
                    
                    # Ensure manifests_referenced is a set
                    if 'manifests_referenced' in build_data:
                        if isinstance(build_data['manifests_referenced'], list):
                            build_data['manifests_referenced'] = set(build_data['manifests_referenced'])
                    else:
                        build_data['manifests_referenced'] = set()
                        
                    # Set default timestamp if missing
                    if 'timestamp' not in build_data:
                        build_data['timestamp'] = time.time()
                        
                    # Set default dependencies if missing
                    if 'dependencies' not in build_data:
                        build_data['dependencies'] = []
                        
                    build = ArchivedBuild(**build_data)
                    key = f"{build.game_id}_{build.build_id}_{build.platform}"
                    self.archived_builds[key] = build
                    
                # Load manifests
                for manifest_data in data.get('manifests', []):
                    # Ensure chunks_referenced is a set
                    if 'chunks_referenced' in manifest_data:
                        if isinstance(manifest_data['chunks_referenced'], list):
                            manifest_data['chunks_referenced'] = set(manifest_data['chunks_referenced'])
                    else:
                        manifest_data['chunks_referenced'] = set()
                        
                    # Ensure languages is a set 
                    if 'languages' in manifest_data:
                        if isinstance(manifest_data['languages'], list):
                            manifest_data['languages'] = set(manifest_data['languages'])
                    else:
                        manifest_data['languages'] = set()
                        
                    manifest = ArchivedManifest(**manifest_data)
                    self.archived_manifests[manifest.manifest_id] = manifest
                    
            except Exception as e:
                self.logger.error(f"Failed to load database: {e}")
                
    def save_database(self):
        """Save archive database - ONLY build manifests, no depot manifests/chunks/blobs tracking"""
        data = {
            'builds': [],
            'last_updated': time.time()
        }
        
        # Save builds with only essential fields + metadata - NO manifests_referenced
        for build in self.archived_builds.values():
            build_dict = {
                'game_id': build.game_id,
                'build_id': build.build_id,
                'build_hash': build.build_hash,
                'platform': build.platform,
                'version': build.version,
                'archive_path': build.archive_path,
                'cdn_url': build.cdn_url,
                'repository_id': build.repository_id,
                'version_name': getattr(build, 'version_name', ''),
                'tags': getattr(build, 'tags', [])
                # NO manifests_referenced - file system is truth!
            }
            
            data['builds'].append(build_dict)
            
        # NO manifests section - we don't track depot manifests in database!
        
        with open(self.database_path, 'w') as f:
            json.dump(data, f, indent=2)

    def _save_raw_build_manifest(self, cdn_url: str, raw_data: bytes) -> str:
        """Save raw build manifest data preserving CDN structure"""
        # Debug: Log the URL being processed
        print(f"DEBUG: Processing URL: {cdn_url}")
        
        # Parse CDN URL to get version and path
        # Example: https://gog-cdn-fastly.gog.com/content-system/v2/meta/92/ab/92ab42631ff4742b309bb62c175e6306
        if '/v1/' in cdn_url:
            version = 'v1'
            print(f"DEBUG: Detected V1 URL")
            # v1 URLs are different: /v1/manifests/1207658930/windows/37794096/repository.json
            url_parts = cdn_url.split('/v1/manifests/')
            if len(url_parts) > 1:
                path_part = url_parts[1]  # "1207658930/windows/37794096/repository.json"
                save_path = self.builds_dir / version / "manifests" / path_part
            else:
                # Fallback
                filename = cdn_url.split('/')[-1]
                save_path = self.builds_dir / version / filename
        elif '/v2/' in cdn_url:
            version = 'v2'
            print(f"DEBUG: Detected V2 URL")
            # v2 URLs: /content-system/v2/meta/92/ab/92ab42631ff4742b309bb62c175e6306
            url_parts = cdn_url.split('/v2/')
            if len(url_parts) > 1:
                path_part = url_parts[1]  # "meta/92/ab/92ab42631ff4742b309bb62c175e6306"
                save_path = self.builds_dir / version / path_part
                print(f"DEBUG: V2 path_part: {path_part}")
                print(f"DEBUG: V2 save_path: {save_path}")
            else:
                # Fallback
                filename = cdn_url.split('/')[-1]
                save_path = self.builds_dir / version / filename
                print(f"DEBUG: V2 fallback, filename: {filename}")
        elif 'downloadable-manifests-collector.gog.com' in cdn_url and '/manifests/builds/' in cdn_url:
            # New pattern: https://downloadable-manifests-collector.gog.com/manifests/builds/2e/18/2e18ff86c77e4960f905a9e5a1545468
            version = 'v2'  # These appear to be v2 builds based on generation_queried: 2
            print(f"DEBUG: Detected downloadable-manifests-collector URL")
            url_parts = cdn_url.split('/manifests/builds/')
            if len(url_parts) > 1:
                path_part = url_parts[1]  # "2e/18/2e18ff86c77e4960f905a9e5a1545468"
                save_path = self.builds_dir / version / "builds" / path_part
                print(f"DEBUG: builds path_part: {path_part}")
                print(f"DEBUG: builds save_path: {save_path}")
            else:
                # Fallback
                filename = cdn_url.split('/')[-1]
                save_path = self.builds_dir / version / "builds" / filename
                print(f"DEBUG: builds fallback, filename: {filename}")
        else:
            # Unknown version, save in root
            filename = cdn_url.split('/')[-1]
            save_path = self.builds_dir / filename
            print(f"DEBUG: UNKNOWN VERSION - fallback to root, filename: {filename}")

        print(f"DEBUG: Final save_path: {save_path}")

        # Create directories and save raw file
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, 'wb') as f:
            f.write(raw_data)
            
        # Also save prettified JSON copy next to the raw file
        if version == 'v1' and save_path.suffix == '.json':
            # For v1 JSON files, add .pretty suffix before .json to avoid overwriting
            json_path = save_path.with_suffix('.pretty.json')
        else:
            # For v2 files (no extension), add .json suffix
            json_path = save_path.with_suffix('.json')
            
        try:
            # Try to decompress and prettify
            if version == 'v2':
                try:
                    decompressed = zlib.decompress(raw_data, 15)
                    manifest_data = json.loads(decompressed.decode('utf-8'))
                except zlib.error:
                    # Not compressed, try as plain JSON
                    manifest_data = json.loads(raw_data.decode('utf-8'))
            else:
                # v1 manifests are typically plain JSON
                manifest_data = json.loads(raw_data.decode('utf-8'))
                
            with open(json_path, 'w') as f:
                json.dump(manifest_data, f, indent=2)
                
            self.logger.debug(f"Saved prettified build manifest: {json_path}")
        except Exception as e:
            self.logger.warning(f"Failed to create prettified copy: {e}")
            
        self.logger.debug(f"Saved raw build manifest: {save_path}")
        return str(save_path)

    def _save_raw_depot_manifest(self, cdn_url: str, raw_data: bytes) -> str:
        """Save raw depot manifest data preserving CDN structure"""
        # Parse CDN URL to get version and path
        # Example: https://gog-cdn-fastly.gog.com/content-system/v1/manifests/1207658930/windows/37794096/repository.json
        # Example: https://gog-cdn-fastly.gog.com/content-system/v2/depots/db/5f/db5f65c5b09c1ad45c4f88d3e1a9b79f
        if '/v1/' in cdn_url:
            version = 'v1'
            url_parts = cdn_url.split('/v1/')
            if len(url_parts) > 1:
                path_part = url_parts[1]  # "manifests/1207658930/windows/37794096/repository.json"
                save_path = self.manifests_dir / version / path_part
            else:
                filename = cdn_url.split('/')[-1]
                save_path = self.manifests_dir / version / filename
        elif '/v2/' in cdn_url:
            version = 'v2'
            url_parts = cdn_url.split('/v2/')
            if len(url_parts) > 1:
                path_part = url_parts[1]  # "depots/db/5f/db5f65c5b09c1ad45c4f88d3e1a9b79f"
                save_path = self.manifests_dir / version / path_part
            else:
                filename = cdn_url.split('/')[-1]
                save_path = self.manifests_dir / version / filename
        else:
            # Unknown version, save in root
            filename = cdn_url.split('/')[-1]
            save_path = self.manifests_dir / filename

        # Create directories and save
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, 'wb') as f:
            f.write(raw_data)
            
        self.logger.debug(f"Saved raw depot manifest: {save_path}")
        return str(save_path)

    def _load_raw_depot_manifest(self, raw_path: str) -> dict:
        """Load and decompress raw depot manifest"""
        with open(raw_path, 'rb') as f:
            raw_data = f.read()
            
        # Check if it's compressed (most v2 manifests are gzip compressed)
        try:
            import gzip
            if raw_data.startswith(b'\x1f\x8b'):  # gzip magic number
                decompressed = gzip.decompress(raw_data)
                return json.loads(decompressed.decode('utf-8'))
            else:
                # Try as plain JSON
                return json.loads(raw_data.decode('utf-8'))
        except Exception as e:
            self.logger.error(f"Failed to load raw depot manifest from {raw_path}: {e}")
            return None

    def _save_raw_chunk(self, content_id: str, raw_data: bytes) -> str:
        """Save raw chunk data preserving CDN structure: chunks/[2 chars]/[2 chars]/[full_md5]"""
        # v2 chunks use compressedMd5 hash structure to match CDN paths exactly
        if len(content_id) >= 4:
            prefix1 = content_id[:2]
            prefix2 = content_id[2:4]
            save_path = self.chunks_dir / prefix1 / prefix2 / content_id
        else:
            save_path = self.chunks_dir / content_id
            
        # Create directories and save
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, 'wb') as f:
            f.write(raw_data)
            
        self.logger.debug(f"Saved raw chunk: {save_path}")
        return str(save_path)

    def _save_raw_blob(self, blob_id: str, raw_data: bytes) -> str:
        """Save raw blob data preserving CDN structure"""
        # v1 blobs might have different structure, save by blob ID
        if len(blob_id) >= 4:
            prefix1 = blob_id[:2]
            prefix2 = blob_id[2:4]
            save_path = self.blobs_dir / prefix1 / prefix2 / blob_id
        else:
            save_path = self.blobs_dir / blob_id
            
        # Create directories and save
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, 'wb') as f:
            f.write(raw_data)
            
        self.logger.debug(f"Saved raw blob: {save_path}")
        return str(save_path)

    def _load_raw_build_manifest(self, raw_path: str) -> dict:
        """Load and decompress raw build manifest"""
        with open(raw_path, 'rb') as f:
            raw_data = f.read()
            
        # Check if it's compressed (most v2 manifests are gzip compressed)
        try:
            import gzip
            if raw_data.startswith(b'\x1f\x8b'):  # gzip magic number
                decompressed = gzip.decompress(raw_data)
                return json.loads(decompressed.decode('utf-8'))
            else:
                # Try as plain JSON
                return json.loads(raw_data.decode('utf-8'))
        except Exception as e:
            self.logger.error(f"Failed to load raw manifest from {raw_path}: {e}")
            return None
            
    def archive_game_manifests(self, game_id: str, platforms: List[str] = None) -> List[ArchivedBuild]:
        """Archive all manifests for a game across platforms and builds"""
        if not platforms:
            platforms = ['windows', 'osx', 'linux']
            
        archived = []
        
        for platform in platforms:
            try:
                # Get builds for this platform (comprehensive - both V1 and V2)
                builds_data = self.api_handler.session.get(
                    f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds"
                )
                
                if not builds_data.ok:
                    continue
                    
                builds = builds_data.json()
                
                for build in builds['items']:
                    build_id = build['build_id']
                    
                    # Check if we already have this manifest
                    manifest_key = f"{game_id}_{build_id}_{platform}"
                    if manifest_key in self.archived_builds:
                        self.logger.info(f"Already archived: {manifest_key}")
                        continue
                        
                    # Download and archive the manifest
                    raw_response = self.api_handler.session.get(build['link'])
                    if raw_response.ok:
                        raw_data = raw_response.content
                        
                        # Decompress for processing
                        try:
                            manifest_data = json.loads(zlib.decompress(raw_data, 15))
                        except zlib.error:
                            manifest_data = raw_response.json()
                            raw_data = json.dumps(manifest_data).encode('utf-8')  # Store as raw JSON if not compressed
                        
                        archived_manifest = self._archive_manifest(
                            game_id, build_id, platform, manifest_data, build['link'], raw_data,
                            version_name=build.get('version_name', ''),
                            tags=build.get('tags', []),
                            repository_id=build.get('legacy_build_id')
                        )
                        if archived_manifest:
                            archived.append(archived_manifest)
                    else:
                        self.logger.warning(f"Failed to download manifest for {game_id}/{build_id}/{platform} - URL may be invalid: {build['link']}")
                            
            except Exception as e:
                self.logger.error(f"Failed to archive manifests for {game_id}/{platform}: {e}")
                
        return archived
        
    def archive_build_manifests(self, game_id: str, build_id: str, platforms: List[str] = None) -> List[ArchivedBuild]:
        """Archive manifests for a specific build"""
        if not platforms:
            platforms = ['windows']
            
        archived = []
        
        for platform in platforms:
            try:
                # Search both generations to find the specific build
                target_build = None
                for generation in [1, 2]:
                    if generation == 1:
                        # Generation 1: omit generation parameter
                        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds"
                    else:
                        # Generation 2: explicit generation=2
                        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds?generation=2"
                    
                    builds_data = self.api_handler.session.get(url)
                    
                    if not builds_data.ok:
                        continue
                        
                    builds = builds_data.json()
                    
                    # Find the specific build
                    for build in builds['items']:
                        if build['build_id'] == build_id:
                            target_build = build
                            break
                    
                    if target_build:
                        break  # Found the build, no need to check other generations
                        
                if not target_build:
                    self.logger.warning(f"Build {build_id} not found for {game_id}/{platform}")
                    continue
                
                # Check if we already have this manifest
                manifest_key = f"{game_id}_{build_id}_{platform}"
                if manifest_key in self.archived_builds:
                    self.logger.info(f"Already archived: {manifest_key}")
                    archived.append(self.archived_builds[manifest_key])
                    continue
                    
                # Download and archive the manifest
                raw_response = self.api_handler.session.get(target_build['link'])
                if raw_response.ok:
                    raw_data = raw_response.content
                    
                    # Decompress for processing
                    try:
                        manifest_data = json.loads(zlib.decompress(raw_data, 15))
                    except zlib.error:
                        manifest_data = raw_response.json()
                        raw_data = json.dumps(manifest_data).encode('utf-8')  # Store as raw JSON if not compressed
                    
                    archived_manifest = self._archive_manifest(
                        game_id, build_id, platform, manifest_data, target_build['link'], raw_data,
                        version_name=target_build.get('version_name', ''),
                        tags=target_build.get('tags', []),
                        repository_id=target_build.get('legacy_build_id')
                    )
                    if archived_manifest:
                        archived.append(archived_manifest)
                else:
                    self.logger.warning(f"Failed to download manifest for {game_id}/{build_id}/{platform}")
                        
            except Exception as e:
                self.logger.error(f"Failed to archive build manifest for {game_id}/{build_id}/{platform}: {e}")
                
        # Save database after archiving build manifests
        if archived:
            self.save_database()
                
        return archived

    def archive_repository_build_manifests(self, game_id: str, repository_id: str, repository_version: int, platforms: List[str] = None) -> List[ArchivedBuild]:
        """Archive build manifests using repository ID and API version
        
        Note: Repository IDs are platform-specific, but the API still requires a platform in the URL.
        We try platforms until we find the one that works for this repository ID.
        """
        if not platforms:
            platforms = ['windows', 'osx', 'linux']  # Try all platforms to find the right one
            
        archived = []
        
        # Check if we already have this repository archived for any platform
        existing_build = None
        for build_key, archived_build in self.archived_builds.items():
            if (archived_build.game_id == game_id and 
                archived_build.repository_id == repository_id):
                existing_build = archived_build
                break
        
        if existing_build:
            self.logger.info(f"Repository already archived: {game_id}_{repository_id}_{existing_build.platform}")
            return [existing_build]
        
        # Try each platform until we find one that works
        for platform in platforms:
            try:
                # Build repository URL based on API version
                if repository_version == 1:
                    # V1 API: legacy builds using repository ID
                    url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds/{repository_id}/repository"
                elif repository_version == 2:
                    # V2 API: newer builds using repository ID with generation=2
                    url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds/{repository_id}/repository?generation=2"
                else:
                    raise ValueError(f"Unsupported repository version: {repository_version}")
                
                self.logger.info(f"Repository URL (V{repository_version}): {url}")
                    
                # Download repository manifest
                raw_response = self.api_handler.session.get(url)
                if raw_response.ok:
                    raw_data = raw_response.content
                    
                    # Decompress for processing
                    try:
                        if repository_version == 2:
                            # V2 manifests are usually compressed
                            manifest_data = json.loads(zlib.decompress(raw_data, 15))
                        else:
                            # V1 manifests might be plain JSON
                            try:
                                manifest_data = json.loads(zlib.decompress(raw_data, 15))
                            except zlib.error:
                                manifest_data = raw_response.json()
                                raw_data = json.dumps(manifest_data).encode('utf-8')
                    except Exception as e:
                        self.logger.error(f"Failed to decompress repository manifest: {e}")
                        continue
                    
                    # Extract build_id from repository manifest
                    build_id = repository_id  # For now, use repository_id as build_id
                    if 'buildId' in manifest_data:
                        build_id = manifest_data['buildId']
                    elif 'build_id' in manifest_data:
                        build_id = manifest_data['build_id']
                    
                    archived_manifest = self._archive_manifest(
                        game_id, build_id, platform, manifest_data, url, raw_data,
                        version_name=manifest_data.get('versionName', ''),
                        tags=manifest_data.get('tags', []),
                        repository_id=repository_id
                    )
                    if archived_manifest:
                        archived.append(archived_manifest)
                        # Found the repository - no need to try other platforms
                        break
                else:
                    self.logger.debug(f"Repository not found for platform {platform} - Status: {raw_response.status_code}")
                        
            except Exception as e:
                self.logger.debug(f"Failed to check repository for platform {platform}: {e}")
                
        # Save database after archiving repository manifests
        if archived:
            self.save_database()
                
        return archived

    def archive_repository_and_depot_manifests_only(self, game_id: str, repository_id: str, repository_version: int, platforms: List[str] = None) -> Dict:
        """Archive repository build manifests and all their referenced depot manifests - no chunks/blobs"""
        if not platforms:
            platforms = ['windows']
            
        results = {
            'game_id': game_id,
            'repository_id': repository_id,
            'repository_version': repository_version,
            'builds_archived': 0,
            'depot_manifests_archived': 0,
            'depot_manifests_skipped': 0,
            'errors': []
        }
        
        try:
            # First, archive the repository build manifests
            archived_builds = self.archive_repository_build_manifests(game_id, repository_id, repository_version, platforms)
            results['builds_archived'] = len(archived_builds)
            
            if not archived_builds:
                results['errors'].append(f"No repository manifests found for {game_id}/{repository_id}")
                return results
            
            # Now download all depot manifests referenced by the repository build manifests
            depot_manifests_to_download = set()
            
            for archived_build in archived_builds:
                print(f"\n=== Processing Repository {repository_id} (v{archived_build.version}) ===")
                print(f"Platform: {archived_build.platform}")
                
                # Read the build manifest file directly to extract depot manifest IDs
                build_manifest_path = self.archive_root / archived_build.archive_path
                try:
                    with open(build_manifest_path, 'rb') as f:
                        raw_data = f.read()
                    
                    # Decompress if needed (V2 manifests are usually compressed)
                    if archived_build.version == 2:
                        if raw_data.startswith(b'\x1f\x8b'):  # gzip
                            manifest_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
                        elif raw_data.startswith(b'\x78'):  # zlib
                            manifest_data = json.loads(zlib.decompress(raw_data).decode('utf-8'))
                        else:
                            manifest_data = json.loads(raw_data.decode('utf-8'))
                    else:
                        # V1 manifests are plain JSON
                        manifest_data = json.loads(raw_data.decode('utf-8'))
                    
                    # Extract depot manifest IDs from the build manifest
                    depot_manifest_ids = []
                    if archived_build.version == 2:
                        # V2: depot manifests are in depots array
                        for depot in manifest_data.get('depots', []):
                            if 'manifest' in depot:
                                depot_manifest_ids.append(depot['manifest'])
                    else:
                        # V1: depot manifests are in product.depots array
                        product_data = manifest_data.get('product', {})
                        for depot in product_data.get('depots', []):
                            if 'manifest' in depot:
                                depot_manifest_ids.append(depot['manifest'])
                    
                    print(f"Found {len(depot_manifest_ids)} depot manifests to download")
                    
                    # Add to download set
                    for manifest_id in depot_manifest_ids:
                        depot_manifests_to_download.add((manifest_id, archived_build.version, archived_build.platform, archived_build.repository_id))
                        
                except Exception as e:
                    error_msg = f"Failed to read repository manifest {archived_build.archive_path}: {e}"
                    print(f"❌ {error_msg}")
                    results['errors'].append(error_msg)
            
            print(f"\n=== Depot Manifest Download Summary ===")
            print(f"Total depot manifests to process: {len(depot_manifests_to_download)}")
            
            # Download each depot manifest (but not its chunks/blobs)
            for manifest_id, version, platform, repo_id in depot_manifests_to_download:
                print(f"\n📥 Processing depot manifest: {manifest_id}")
                print(f"   Version: v{version}")
                print(f"   Platform: {platform}")
                
                # Check if we already have this depot manifest on disk
                if version == 2:
                    # Check v2 depot manifest path
                    galaxy_path = manifest_id if "/" in manifest_id else f"{manifest_id[0:2]}/{manifest_id[2:4]}/{manifest_id}"
                    depot_path = self.archive_root / "manifests" / "v2" / "depots" / galaxy_path
                    expected_url = f"https://downloadable-manifests-collector.gog.com/manifests/depots/{galaxy_path}"
                else:
                    # Check v1 depot manifest path  
                    depot_path = self.archive_root / "manifests" / "v1" / "manifests" / game_id / platform / repo_id / manifest_id
                    expected_url = f"https://gog-cdn-fastly.gog.com/content-system/v1/manifests/{game_id}/{platform}/{repo_id}/{manifest_id}"
                
                print(f"   🌐 Expected URL: {expected_url}")
                print(f"   📁 Expected path: {depot_path}")
                
                if depot_path.exists():
                    print(f"   ✅ Depot manifest already exists on disk - SKIPPING")
                    results['depot_manifests_skipped'] += 1
                    continue
                
                if version == 2:
                    # Download v2 depot manifest (but skip chunks)
                    depot_result = self._download_v2_depot_manifest_only(game_id, manifest_id)
                else:
                    # Download v1 depot manifest (but skip blob)
                    depot_result = self._download_v1_depot_manifest_only(game_id, platform, archived_build.build_id, repo_id, manifest_id)
                
                if depot_result['success']:
                    if depot_result.get('already_exists'):
                        results['depot_manifests_skipped'] += 1
                        print(f"⚡ Depot manifest already exists: {manifest_id}")
                    else:
                        results['depot_manifests_archived'] += 1
                        print(f"✅ Successfully archived depot manifest: {manifest_id}")
                        if 'chunks_found' in depot_result and 'files_found' in depot_result:
                            print(f"   📊 {depot_result['files_found']} files, {depot_result['total_size']:,} bytes, {depot_result['chunks_found']} chunks")
                else:
                    results['errors'].extend(depot_result['errors'])
                    print(f"❌ Failed to archive depot manifest: {manifest_id}")
            
            # Save database after processing all manifests
            self.save_database()
            print(f"\n=== Repository Manifests-Only Complete ===")
            print(f"Repository manifests: {results['builds_archived']}")
            print(f"Depot manifests downloaded: {results['depot_manifests_archived']}")
            print(f"Depot manifests skipped (already archived): {results['depot_manifests_skipped']}")
            
        except Exception as e:
            error_msg = f"Failed to archive manifests for repository {repository_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results

    def verify_and_download_chunks_for_repository(self, game_id: str, repository_id: str, repository_version: int, platforms: List[str] = None) -> Dict:
        """Scan all depot manifests for a repository, deduplicate chunks, verify integrity, and download missing/corrupted chunks"""
        if not platforms:
            platforms = ['windows']
            
        results = {
            'game_id': game_id,
            'repository_id': repository_id,
            'repository_version': repository_version,
            'total_chunks_found': 0,
            'chunks_verified_ok': 0,
            'chunks_missing': 0,
            'chunks_corrupted': 0,
            'chunks_downloaded': 0,
            'errors': []
        }
        
        try:
            # First, find the repository build
            archived_builds = self.archive_repository_build_manifests(game_id, repository_id, repository_version, platforms)
            if not archived_builds:
                results['errors'].append(f"No repository manifests found for {game_id}/{repository_id}")
                return results
            
            # Collect all chunks from all depot manifests (deduplicated)
            all_chunks = {}  # chunk_id -> chunk_info
            
            for archived_build in archived_builds:
                print(f"\n=== Scanning Repository {repository_id} for Chunks ===")
                print(f"Platform: {archived_build.platform}")
                
                # Read the build manifest file to get depot manifest IDs
                build_manifest_path = self.archive_root / archived_build.archive_path
                try:
                    with open(build_manifest_path, 'rb') as f:
                        raw_data = f.read()
                    
                    # Decompress if needed
                    if archived_build.version == 2:
                        if raw_data.startswith(b'\x1f\x8b'):  # gzip
                            manifest_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
                        elif raw_data.startswith(b'\x78'):  # zlib
                            manifest_data = json.loads(zlib.decompress(raw_data).decode('utf-8'))
                        else:
                            manifest_data = json.loads(raw_data.decode('utf-8'))
                    else:
                        # V1 manifests are plain JSON
                        manifest_data = json.loads(raw_data.decode('utf-8'))
                    
                    # Extract depot manifest IDs
                    depot_manifest_ids = []
                    if archived_build.version == 2:
                        for depot in manifest_data.get('depots', []):
                            if 'manifest' in depot:
                                depot_manifest_ids.append(depot['manifest'])
                    else:
                        product_data = manifest_data.get('product', {})
                        for depot in product_data.get('depots', []):
                            if 'manifest' in depot:
                                depot_manifest_ids.append(depot['manifest'])
                    
                    print(f"Found {len(depot_manifest_ids)} depot manifests to scan for chunks")
                    
                    # Scan each depot manifest for chunks
                    for manifest_id in depot_manifest_ids:
                        print(f"   📋 Scanning depot manifest: {manifest_id}")
                        chunks_from_manifest = self._extract_chunks_from_depot_manifest(game_id, manifest_id, archived_build.version)
                        print(f"      Found {len(chunks_from_manifest)} chunks in this manifest")
                        for chunk_id, chunk_info in chunks_from_manifest.items():
                            if chunk_id not in all_chunks:
                                all_chunks[chunk_id] = chunk_info
                                
                except Exception as e:
                    error_msg = f"Failed to read repository manifest {archived_build.archive_path}: {e}"
                    print(f"❌ {error_msg}")
                    results['errors'].append(error_msg)
            
            results['total_chunks_found'] = len(all_chunks)
            print(f"\n=== Chunk Verification Summary ===")
            print(f"Total unique chunks found: {len(all_chunks)}")
            
            if not all_chunks:
                print("No chunks found to verify")
                return results
            
            # Now verify each chunk and download missing/corrupted ones
            print(f"\n🔍 Verifying {len(all_chunks)} chunks...")
            chunk_count = 0
            for chunk_id, chunk_info in all_chunks.items():
                chunk_count += 1
                
                # Show detailed verification for every chunk
                print(f"   🔍 [{chunk_count}/{len(all_chunks)}] Verifying chunk: {chunk_id}")
                
                chunk_status = self._verify_chunk_integrity(chunk_id, chunk_info)
                
                # Show the result of each verification
                if chunk_status == 'ok':
                    print(f"      ✅ Chunk exists and is valid")
                elif chunk_status == 'missing':
                    print(f"      ❌ Chunk is missing")
                elif chunk_status == 'corrupted':
                    print(f"      ⚠️  Chunk is corrupted (size mismatch)")
                
                if chunk_status == 'ok':
                    results['chunks_verified_ok'] += 1
                elif chunk_status == 'missing':
                    results['chunks_missing'] += 1
                    # Download missing chunk
                    print(f"📥 Downloading missing chunk: {chunk_id}")
                    if self._download_chunk(chunk_id, chunk_info):
                        results['chunks_downloaded'] += 1
                        print(f"✅ Downloaded missing chunk: {chunk_id}")
                    else:
                        manifest_id = chunk_info.get('manifest_id', 'unknown')
                        file_path = chunk_info.get('file_path', 'unknown')
                        error_msg = f"Failed to download missing chunk: {chunk_id} from manifest {manifest_id} (file: {file_path})"
                        results['errors'].append(error_msg)
                        print(f"❌ {error_msg}")
                elif chunk_status == 'corrupted':
                    results['chunks_corrupted'] += 1
                    # Re-download corrupted chunk
                    print(f"🔄 Re-downloading corrupted chunk: {chunk_id}")
                    if self._download_chunk(chunk_id, chunk_info):
                        results['chunks_downloaded'] += 1
                        print(f"✅ Re-downloaded corrupted chunk: {chunk_id}")
                    else:
                        manifest_id = chunk_info.get('manifest_id', 'unknown')
                        file_path = chunk_info.get('file_path', 'unknown')
                        error_msg = f"Failed to re-download corrupted chunk: {chunk_id} from manifest {manifest_id} (file: {file_path})"
                        results['errors'].append(error_msg)
                        print(f"❌ {error_msg}")
            
            print(f"\n=== Chunk Verification Complete ===")
            print(f"Chunks verified OK: {results['chunks_verified_ok']}")
            print(f"Chunks missing: {results['chunks_missing']}")
            print(f"Chunks corrupted: {results['chunks_corrupted']}")
            print(f"Chunks downloaded: {results['chunks_downloaded']}")
            
        except Exception as e:
            error_msg = f"Failed to verify chunks for repository {repository_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
        
    def _extract_chunks_from_depot_manifest(self, game_id: str, manifest_id: str, version: int) -> Dict[str, Dict]:
        """Extract all chunks from a depot manifest file on disk"""
        chunks = {}
        
        try:
            # Find the depot manifest file on disk
            if version == 2:
                galaxy_path = manifest_id if "/" in manifest_id else f"{manifest_id[0:2]}/{manifest_id[2:4]}/{manifest_id}"
                # Check both possible locations
                depot_paths = [
                    self.archive_root / "manifests" / "v2" / "depots" / galaxy_path,
                    self.archive_root / "manifests" / "v2" / "meta" / galaxy_path
                ]
            else:
                # V1 depot manifest paths are more complex, need platform/repository context
                # For now, skip V1 chunk extraction (can be added later if needed)
                return chunks
            
            depot_manifest_path = None
            for path in depot_paths:
                if path.exists():
                    depot_manifest_path = path
                    break
            
            if not depot_manifest_path:
                self.logger.warning(f"Depot manifest {manifest_id} not found on disk")
                return chunks
            
            # Load and parse the depot manifest
            with open(depot_manifest_path, 'rb') as f:
                raw_data = f.read()
            
            # Decompress if needed
            try:
                import gzip
                if raw_data.startswith(b'\x1f\x8b'):  # gzip
                    decompressed_data = gzip.decompress(raw_data)
                    depot_manifest = json.loads(decompressed_data.decode('utf-8'))
                elif raw_data.startswith(b'\x78'):  # zlib
                    decompressed_data = zlib.decompress(raw_data, 15)
                    depot_manifest = json.loads(decompressed_data.decode('utf-8'))
                else:
                    depot_manifest = json.loads(raw_data.decode('utf-8'))
            except Exception as e:
                self.logger.error(f"Failed to parse depot manifest {manifest_id}: {e}")
                return chunks
            
            # Extract chunks from all files in the depot
            depot_data = depot_manifest.get('depot', {})
            for file_record in depot_data.get('items', []):
                if file_record.get('type') == 'DepotFile':
                    for chunk in file_record.get('chunks', []):
                        chunk_id = chunk.get('compressedMd5')
                        if chunk_id:
                            chunks[chunk_id] = {
                                'compressed_size': chunk.get('compressedSize', 0),
                                'uncompressed_size': chunk.get('size', 0),
                                'offset': chunk.get('offset', 0),
                                'file_path': file_record.get('path', ''),
                                'manifest_id': manifest_id,
                                'game_id': game_id  # Include game_id for secure link generation
                            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract chunks from depot manifest {manifest_id}: {e}")
        
        return chunks
    
    def _verify_chunk_integrity(self, chunk_id: str, chunk_info: Dict) -> str:
        """Verify if a chunk exists and has correct integrity
        
        Returns: 'ok', 'missing', or 'corrupted'
        """
        try:
            # Build chunk file path
            chunk_path = self.chunks_dir / chunk_id[:2] / chunk_id[2:4] / chunk_id
            
            print(f"         🔍 Checking file: {chunk_path}")
            
            if not chunk_path.exists():
                print(f"         ❌ File does not exist")
                return 'missing'
            
            # Verify file size matches expected compressed size
            actual_size = chunk_path.stat().st_size
            expected_size = chunk_info.get('compressed_size', 0)
            
            print(f"         📏 File size: {actual_size} bytes (expected: {expected_size} bytes)")
            
            if expected_size > 0 and actual_size != expected_size:
                print(f"         ⚠️  Size mismatch!")
                return 'corrupted'
            
            print(f"         ✅ File exists and size matches")
            # TODO: Could add MD5 hash verification here if needed
            # For now, size check is sufficient
            return 'ok'
            
        except Exception as e:
            print(f"         💥 Exception during verification: {e}")
            self.logger.error(f"Failed to verify chunk {chunk_id}: {e}")
            return 'corrupted'
    
    def _download_chunk(self, chunk_id: str, chunk_info: Dict) -> bool:
        """Download a missing or corrupted chunk using secure links"""
        try:
            game_id = chunk_info.get('game_id')
            if not game_id:
                self.logger.error(f"No game_id in chunk_info for chunk {chunk_id}")
                return False
            
            # Get secure links for the game to get the correct CDN URL with authentication
            # from gogdl.dl import dl_utils
            secure_links = dl_utils.get_secure_link(
                self.api_handler, "/", game_id, generation=2
            )
            
            if not secure_links:
                self.logger.error(f"Failed to get secure links for game {game_id}")
                return False
            
            # DEBUG: Log the secure links we received
            print(f"DEBUG: Secure links for game {game_id}:")
            for i, link in enumerate(secure_links):
                print(f"  {i}: url_format='{link.get('url_format', 'N/A')}'")
                print(f"     parameters={link.get('parameters', {})}")
            
            # Build chunk URL using secure links (like the existing _download_v2_chunks method)
            base_endpoint = secure_links[0]  # Use first endpoint
            endpoint = base_endpoint.copy()
            
            # Build the chunk path using the correct store structure (not /chunks/)
            # Working path: /content-system/v2/store/{game_id}/{chunk_id[:2]}/{chunk_id[2:4]}/{chunk_id}
            chunk_path = f"/content-system/v2/store/{game_id}/{chunk_id[:2]}/{chunk_id[2:4]}/{chunk_id}"
            endpoint["parameters"]["path"] = chunk_path
            
            print(f"DEBUG: Modified endpoint parameters: {endpoint['parameters']}")
            
            # Merge URL with parameters to get the final authenticated URL
            chunk_url = dl_utils.merge_url_with_params(
                endpoint["url_format"], endpoint["parameters"]
            )
            
            print(f"DEBUG: Final chunk URL: {chunk_url}")
            print(f"DEBUG: Chunk info: manifest_id={chunk_info.get('manifest_id')}, file_path={chunk_info.get('file_path')}")
            
            # Extract just the domain to see what CDN we're hitting
            cdn_domain = chunk_url.split('/')[2] if '://' in chunk_url else 'unknown'
            print(f"DEBUG: CDN Domain: {cdn_domain}")
            
            self.logger.debug(f"Downloading chunk {chunk_id} from: {chunk_url}")
            
            response = self.api_handler.session.get(chunk_url)
            print(f"DEBUG: Response status: {response.status_code}")
            if not response.ok:
                print(f"DEBUG: Response headers: {dict(response.headers)}")
                if response.text:
                    print(f"DEBUG: Response body: {response.text[:200]}...")
            
            if response.ok:
                raw_data = response.content
                
                # Verify size if available
                expected_size = chunk_info.get('compressed_size', 0)
                if expected_size > 0 and len(raw_data) != expected_size:
                    self.logger.warning(f"Downloaded chunk {chunk_id} size mismatch: expected {expected_size}, got {len(raw_data)}")
                
                # Save chunk to disk
                self._save_raw_chunk(chunk_id, raw_data)
                return True
            else:
                self.logger.error(f"Failed to download chunk {chunk_id}: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to download chunk {chunk_id}: {e}")
            return False
        
    def archive_build_and_depot_manifests_only(self, game_id: str, build_id: str, platforms: List[str] = None) -> Dict:
        """Archive build manifests and all their referenced depot manifests - no chunks/blobs"""
        if not platforms:
            platforms = ['windows']
            
        results = {
            'game_id': game_id,
            'build_id': build_id,
            'builds_archived': 0,
            'depot_manifests_archived': 0,
            'depot_manifests_skipped': 0,
            'errors': []
        }
        
        try:
            # First, archive the build manifests
            archived_builds = self.archive_build_manifests(game_id, build_id, platforms)
            results['builds_archived'] = len(archived_builds)
            
            if not archived_builds:
                results['errors'].append(f"No build manifests found for {game_id}/{build_id}")
                return results
            
            # Now download all depot manifests referenced by the build manifests
            depot_manifests_to_download = set()
            
            for archived_build in archived_builds:
                print(f"\n=== Processing Build {build_id} (v{archived_build.version}) ===")
                print(f"Platform: {archived_build.platform}")
                
                # Read the build manifest file directly to extract depot manifest IDs
                build_manifest_path = self.archive_root / archived_build.archive_path
                try:
                    with open(build_manifest_path, 'rb') as f:
                        raw_data = f.read()
                    
                    # Decompress if needed (V2 manifests are usually compressed)
                    if archived_build.version == 2:
                        if raw_data.startswith(b'\x1f\x8b'):  # gzip
                            import gzip
                            manifest_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
                        elif raw_data.startswith(b'\x78'):  # zlib
                            import zlib
                            manifest_data = json.loads(zlib.decompress(raw_data).decode('utf-8'))
                        else:
                            manifest_data = json.loads(raw_data.decode('utf-8'))
                    else:
                        # V1 manifests are plain JSON
                        manifest_data = json.loads(raw_data.decode('utf-8'))
                    
                    # Extract depot manifest IDs from the build manifest
                    depot_manifest_ids = []
                    if archived_build.version == 2:
                        # V2: depot manifests are in depots array
                        for depot in manifest_data.get('depots', []):
                            if 'manifest' in depot:
                                depot_manifest_ids.append(depot['manifest'])
                    else:
                        # V1: depot manifests are in product.depots array
                        product_data = manifest_data.get('product', {})
                        for depot in product_data.get('depots', []):
                            if 'manifest' in depot:
                                depot_manifest_ids.append(depot['manifest'])
                    
                    print(f"Found {len(depot_manifest_ids)} depot manifests to download")
                    
                    # Add to download set
                    for manifest_id in depot_manifest_ids:
                        depot_manifests_to_download.add((manifest_id, archived_build.version, archived_build.platform, archived_build.repository_id))
                        
                except Exception as e:
                    error_msg = f"Failed to read build manifest {archived_build.archive_path}: {e}"
                    print(f"❌ {error_msg}")
                    results['errors'].append(error_msg)
            
            print(f"\n=== Depot Manifest Download Summary ===")
            print(f"Total depot manifests to process: {len(depot_manifests_to_download)}")
            
            # Download each depot manifest (but not its chunks/blobs)
            for manifest_id, version, platform, repository_id in depot_manifests_to_download:
                print(f"\n📥 Processing depot manifest: {manifest_id}")
                print(f"   Version: v{version}")
                print(f"   Platform: {platform}")
                
                # Check if we already have this depot manifest on disk
                if version == 2:
                    # Check v2 depot manifest path
                    galaxy_path = manifest_id if "/" in manifest_id else f"{manifest_id[0:2]}/{manifest_id[2:4]}/{manifest_id}"
                    depot_path = self.archive_root / "manifests" / "v2" / "depots" / galaxy_path
                    expected_url = f"https://downloadable-manifests-collector.gog.com/manifests/depots/{galaxy_path}"
                else:
                    # Check v1 depot manifest path  
                    depot_path = self.archive_root / "manifests" / "v1" / "manifests" / game_id / platform / repository_id / manifest_id
                    expected_url = f"https://gog-cdn-fastly.gog.com/content-system/v1/manifests/{game_id}/{platform}/{repository_id}/{manifest_id}"
                
                print(f"   🌐 Expected URL: {expected_url}")
                print(f"   📁 Expected path: {depot_path}")
                
                if depot_path.exists():
                    print(f"   ✅ Depot manifest already exists on disk - SKIPPING")
                    results['depot_manifests_skipped'] += 1
                    continue
                
                if version == 2:
                    # Download v2 depot manifest (but skip chunks)
                    depot_result = self._download_v2_depot_manifest_only(game_id, manifest_id)
                else:
                    # Download v1 depot manifest (but skip blob)
                    depot_result = self._download_v1_depot_manifest_only(game_id, platform, build_id, repository_id, manifest_id)
                
                if depot_result['success']:
                    results['depot_manifests_archived'] += 1
                    print(f"✅ Successfully archived depot manifest: {manifest_id}")
                else:
                    results['errors'].extend(depot_result['errors'])
                    print(f"❌ Failed to archive depot manifest: {manifest_id}")
            
            # Save database after processing all manifests
            self.save_database()
            print(f"\n=== Manifests-Only Complete ===")
            print(f"Build manifests: {results['builds_archived']}")
            print(f"Depot manifests downloaded: {results['depot_manifests_archived']}")
            print(f"Depot manifests skipped (already archived): {results['depot_manifests_skipped']}")
            
        except Exception as e:
            error_msg = f"Failed to archive manifests for build {build_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
        
    def archive_build_manifests_only(self, game_id: str, build_id: str, platforms: List[str] = None) -> Dict:
        """Archive build manifests only (dry run) - no content download"""
        if not platforms:
            platforms = ['windows']
            
        results = {
            'game_id': game_id,
            'build_id': build_id,
            'builds_archived': 0,
            'manifests_processed': 0,
            'content_summary': {
                'v1_blobs_found': 0,
                'v2_chunks_found': 0,
                'estimated_blob_size': 0,
                'estimated_chunk_count': 0
            },
            'errors': []
        }
        
        try:
            # Archive build manifests first
            archived_builds = self.archive_build_manifests(game_id, build_id, platforms)
            results['builds_archived'] = len(archived_builds)
            
            for archived_build in archived_builds:
                print(f"\n=== Dry Run Analysis: Build {build_id} ===")
                print(f"Version: v{archived_build.version}")
                print(f"Platform: {archived_build.platform}")
                print(f"References {len(archived_build.manifests_referenced)} depot manifests")
                
                depot_count = 0
                for manifest_id in archived_build.manifests_referenced:
                    depot_count += 1
                    print(f"\n{depot_count}. Depot Manifest: {manifest_id}")
                    
                    # Check if we already have this manifest
                    if manifest_id in self.archived_manifests:
                        existing = self.archived_manifests[manifest_id]
                        print(f"   Status: ✅ Already archived")
                        print(f"   Files: {existing.file_count}")
                        print(f"   Size: {existing.total_size:,} bytes ({existing.total_size / (1024**3):.2f} GB)")
                        
                        # Check for blob references
                        if existing.chunks_referenced:
                            for chunk_ref in existing.chunks_referenced:
                                if chunk_ref in self.archived_blobs:
                                    blob = self.archived_blobs[chunk_ref]
                                    blob_path = Path(blob.archive_path)
                                    if blob_path.exists():
                                        print(f"   Blob: ✅ {chunk_ref} exists ({blob.total_size:,} bytes)")
                                        results['content_summary']['v1_blobs_found'] += 1
                                        results['content_summary']['estimated_blob_size'] += blob.total_size
                                    else:
                                        print(f"   Blob: ❌ {chunk_ref} missing - would re-download")
                        
                        if existing.version == 2 and len(existing.chunks_referenced) > 0:
                            results['content_summary']['v2_chunks_found'] += len(existing.chunks_referenced)
                            print(f"   Chunks: {len(existing.chunks_referenced)} v2 chunks referenced")
                    else:
                        print(f"   Status: DOWNLOAD - Would download and process")
                        if archived_build.version == 1:
                            print(f"   Content: Would download v1 blob")
                        else:
                            print(f"   Content: Would download v2 chunks")
                    
                    results['manifests_processed'] += 1
                
        except Exception as e:
            error_msg = f"Failed to analyze build {build_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
        
    def _archive_manifest(self, game_id: str, build_id: str, platform: str, 
                         manifest_data: dict, cdn_url: str, raw_data: bytes,
                         version_name: str = "", tags: List[str] = None,
                         repository_id: str = None) -> Optional[ArchivedBuild]:
        """Archive a single manifest"""
        try:
            # Validate input
            if not manifest_data:
                self.logger.error(f"Cannot archive manifest {game_id}/{build_id}/{platform}: manifest_data is None")
                return None
                
            # Save raw manifest data preserving CDN structure (this also saves prettified copy)
            raw_path = self._save_raw_build_manifest(cdn_url, raw_data)
            
            # Create build hash from raw file data (more reliable than JSON content hash)
            build_hash = hashlib.sha256(raw_data).hexdigest()
            
            # Convert to relative path (relative to archive_root)
            raw_path_obj = Path(raw_path)
            relative_path = raw_path_obj.relative_to(self.archive_root)
                
            # Extract chunk references and determine content type
            chunks_referenced = set()
            blobs_referenced = set()
            version = manifest_data.get('version', 2)
            
            if version == 2:
                # Parse v2 manifest - extract depot manifests directly from JSON
                for depot in manifest_data.get('depots', []):
                    chunks_referenced.add(depot.get('manifest', ''))
                    
                # Also check offline depot (skip for now - offline depot chunks often fail to download)
                if 'offlineDepot' in manifest_data:
                    self.logger.debug(f"Skipping offline depot manifest reference: {manifest_data['offlineDepot'].get('manifest', '')} (offline depots not supported)")
                    # chunks_referenced.add(manifest_data['offlineDepot'].get('manifest', ''))
            else:
                # Parse v1 manifest - extract depot manifests from product.depots
                product_data = manifest_data.get('product', {})
                for depot in product_data.get('depots', []):
                    # Only add depots that have manifest (skip redist entries that don't have manifest)
                    if 'manifest' in depot:
                        blobs_referenced.add(depot.get('manifest', ''))
                    
                # Also check offline depot if it exists (skip for now - offline depot chunks often fail to download)
                if 'offlineDepot' in product_data:
                    self.logger.debug(f"Skipping offline depot manifest reference: {product_data['offlineDepot'].get('manifest', '')} (offline depots not supported)")
                    # blobs_referenced.add(product_data['offlineDepot'].get('manifest', ''))
                
            # Create archived build record
            archived_build = ArchivedBuild(
                game_id=game_id,
                build_id=build_id,
                build_hash=build_hash,
                platform=platform,
                version=version,
                archive_path=str(relative_path),
                cdn_url=cdn_url,
                timestamp=time.time(),
                dependencies=manifest_data.get('dependencies', []),
                manifests_referenced=chunks_referenced.union(blobs_referenced),  # Combine both for tracking
                repository_id=repository_id,
                version_name=version_name,
                tags=tags or []
            )
                
            # Store in database
            build_key = f"{game_id}_{build_id}_{platform}"
            self.archived_builds[build_key] = archived_build
            
            self.logger.info(f"Archived build: {build_key} with {len(chunks_referenced)} depots and {len(blobs_referenced)} blobs")
            return archived_build
            
        except Exception as e:
            self.logger.error(f"Failed to archive manifest {game_id}/{build_id}/{platform}: {e}")
            return None
            
    def archive_content_for_manifest(self, archived_build: ArchivedBuild, 
                                    max_workers: int = 4, specific_manifest_id: str = None) -> Dict:
        """Archive all content (chunks or blobs) referenced by a build manifest"""
        results = {'chunks_archived': 0, 'blobs_archived': 0, 'depot_manifests_archived': 0, 'errors': []}
        
        # DEDUPLICATION: Collect all chunks from all depot manifests first
        session_chunks_cache = set()  # Track chunks already processed in this session
        
        try:
            # Load the build manifest to get depot manifest IDs (use absolute path)
            absolute_path = self.archive_root / archived_build.archive_path
            with open(absolute_path, 'rb') as f:
                raw_data = f.read()
                
            # Check if we need to decompress (for v2) or read as JSON (for v1)
            if archived_build.version == 2:
                try:
                    # Try to decompress zlib data
                    decompressed = zlib.decompress(raw_data, 15)
                    build_manifest = json.loads(decompressed.decode('utf-8'))
                except zlib.error:
                    # Not compressed, read as plain JSON  
                    build_manifest = json.loads(raw_data.decode('utf-8'))
            else:
                # v1 manifests are typically plain JSON
                build_manifest = json.loads(raw_data.decode('utf-8'))
            
            depot_manifests = []
            
            if archived_build.version == 2:
                # v2 build manifest - get depot manifests
                for depot in build_manifest.get('depots', []):
                    if specific_manifest_id is None or depot['manifest'] == specific_manifest_id:
                        depot_manifests.append({
                            'manifest_id': depot['manifest'],
                            'languages': depot.get('languages', ['*']),
                            'type': 'depot'
                        })
                
                # Also check offline depot (skip for now - offline depot chunks often fail to download)
                if 'offlineDepot' in build_manifest:
                    offline_depot = build_manifest['offlineDepot']
                    if specific_manifest_id is None or offline_depot['manifest'] == specific_manifest_id:
                        self.logger.info(f"Skipping offline depot manifest: {offline_depot['manifest']} (offline depots not supported)")
                        # depot_manifests.append({
                        #     'manifest_id': offline_depot['manifest'],
                        #     'languages': offline_depot.get('languages', ['*']),
                        #     'type': 'offline_depot'
                        # })
            else:
                # v1 build manifest - depots are nested under 'product'
                product_data = build_manifest.get('product', {})
                for depot in product_data.get('depots', []):
                    # Only process depots that have manifest (skip redist entries)
                    if 'manifest' in depot and (specific_manifest_id is None or depot['manifest'] == specific_manifest_id):
                        depot_manifests.append({
                            'manifest_id': depot['manifest'],
                            'languages': depot.get('languages', ['*']),
                            'type': 'depot_v1'
                        })
                        
                # Also check offline depot (skip for now - offline depot chunks often fail to download)
                if 'offlineDepot' in product_data:
                    offline_depot = product_data['offlineDepot']
                    if 'manifest' in offline_depot and (specific_manifest_id is None or offline_depot['manifest'] == specific_manifest_id):
                        self.logger.info(f"Skipping offline depot manifest: {offline_depot['manifest']} (offline depots not supported)")
                        # depot_manifests.append({
                        #     'manifest_id': offline_depot['manifest'],
                        #     'languages': offline_depot.get('languages', ['*']),
                        #     'type': 'offline_depot_v1'
                        # })
            
            self.logger.info(f"Found {len(depot_manifests)} depot manifests to process")
            
            if archived_build.version == 2:
                # Process v2 depot manifests sequentially (existing logic)
                for depot_info in depot_manifests:
                    manifest_id = depot_info['manifest_id']
                    self.logger.info(f"Processing depot manifest: {manifest_id}")
                    
                    # Download and archive v2 depot manifest and its chunks
                    chunks_result = self._archive_v2_depot_manifest_and_chunks(
                        archived_build.game_id, manifest_id, max_workers
                    )
                    results['chunks_archived'] += chunks_result['chunks_archived']
                    results['depot_manifests_archived'] += 1 if chunks_result['success'] else 0
            else:
                # V1 DEDUPLICATION: Process all depot manifests first, then deduplicated blobs
                v1_manifests_processed = 0
                v1_depot_manifests_data = {}
                
                # Step 1: Download all v1 depot manifests first
                self.logger.info(f"V1 DEDUPLICATION: Downloading {len(depot_manifests)} depot manifests...")
                for depot_info in depot_manifests:
                    manifest_id = depot_info['manifest_id']
                    self.logger.info(f"Downloading v1 depot manifest: {manifest_id}")
                    
                    # Download depot manifest only (no blob yet)
                    manifest_result = self._download_v1_depot_manifest_only(
                        archived_build.game_id, archived_build.platform, archived_build.build_id, 
                        archived_build.repository_id, manifest_id
                    )
                    
                    if manifest_result['success']:
                        v1_manifests_processed += 1
                        v1_depot_manifests_data[manifest_id] = manifest_result['depot_manifest_data']
                    else:
                        results['errors'].extend(manifest_result.get('errors', []))
                
                results['depot_manifests_archived'] = v1_manifests_processed
                
                # Step 2: Extract and deduplicate blob URLs from all depot manifests
                unique_blob_urls = set()
                for manifest_id, depot_manifest_data in v1_depot_manifests_data.items():
                    for file_record in depot_manifest_data.get("depot", {}).get("files", []):
                        if file_record.get("url"):
                            unique_blob_urls.add(file_record["url"])
                            
                self.logger.info(f"V1 DEDUPLICATION: Found {len(unique_blob_urls)} unique blob URLs across {len(v1_depot_manifests_data)} depot manifests")
                
                # Step 3: Download each unique blob using the working download method
                for blob_url in unique_blob_urls:
                    self.logger.info(f"Downloading deduplicated v1 blob: {blob_url}")
                    
                    # Use the existing working download method instead of a separate deduplication method
                    blob_path = self.blobs_dir / archived_build.build_id / "main.bin"
                    
                    # Get expected size from server via HEAD request first
                    # from gogdl.dl import dl_utils
                    secure_links = dl_utils.get_secure_link(
                        self.api_handler, f"/{archived_build.platform}/{archived_build.repository_id}/", archived_build.game_id, generation=1
                    )
                    
                    if isinstance(secure_links, str):
                        blob_cdn_url = f"{secure_links}/main.bin"
                    else:
                        endpoint = secure_links[0].copy()
                        endpoint["parameters"]["path"] += "/main.bin"
                        blob_cdn_url = dl_utils.merge_url_with_params(
                            endpoint["url_format"], endpoint["parameters"]
                        )
                    
                    head_response = self.api_handler.session.head(blob_cdn_url, timeout=30)
                    expected_size = int(head_response.headers.get('Content-Length', 0))
                    
                    # Check if we already have this blob complete (deduplication check with size validation)
                    if blob_path.exists():
                        actual_size = blob_path.stat().st_size
                        self.logger.info(f"Found existing blob file: {blob_path} ({actual_size:,} bytes)")
                        
                        # Only skip if file size matches expected size from server
                        if expected_size > 0 and actual_size == expected_size:
                            self.logger.info(f"✅ V1 blob {blob_url} already exists and is complete:")
                            self.logger.info(f"  File: {blob_path}")
                            self.logger.info(f"  Size: {actual_size:,} bytes ({actual_size / (1024**3):.2f} GB)")
                            self.logger.info(f"  Server: {expected_size:,} bytes - ✅ Size matches!")
                            self.logger.info(f"  SKIPPING download to avoid re-download")
                            results['blobs_archived'] += 1
                            continue
                        elif actual_size == 0:
                            self.logger.info(f"⚠️  Blob file exists but is empty (0 bytes) - will download")
                        elif expected_size > 0 and actual_size != expected_size:
                            self.logger.info(f"⚠️  Blob size mismatch:")
                            self.logger.info(f"  Actual: {actual_size:,} bytes ({actual_size / (1024**3):.2f} GB)")
                            self.logger.info(f"  Expected: {expected_size:,} bytes ({expected_size / (1024**3):.2f} GB)")
                            self.logger.info(f"  Will resume/redownload to correct size")
                        else:
                            self.logger.info(f"⚠️  Cannot verify size (server returned {expected_size}) - will download/resume")
                    
                    # Use the existing working download method
                    if self._download_v1_blob_with_resume(archived_build.game_id, archived_build.platform, 
                                                        archived_build.repository_id, archived_build.build_id, 
                                                        blob_path, expected_size):
                        # Generate checksums after successful download
                        self.logger.info("📋 Generating checksums for downloaded blob...")
                        if self._generate_blob_checksum_xml(blob_path, expected_size):
                            self.logger.info("✅ Checksum generation complete")
                        
                        # Create blob tracking record
                        actual_size = blob_path.stat().st_size
                        blob_key = f"{archived_build.game_id}_{archived_build.build_id}_main.bin"
                        
                        archived_blob = ArchivedBlob(
                            depot_manifest=blob_key,
                            secure_url=blob_cdn_url,
                            total_size=actual_size,
                            archive_path=str(blob_path),
                            first_seen=time.time(),
                            last_verified=time.time(),
                            files_contained=[],
                            depot_info={
                                'game_id': archived_build.game_id,
                                'platform': archived_build.platform,
                                'build_id': archived_build.build_id,
                                'estimated_size': expected_size,
                                'actual_size': actual_size,
                                'referenced_by_manifests': list(v1_depot_manifests_data.keys())
                            }
                        )
                        
                        self.archived_blobs[blob_key] = archived_blob
                        results['blobs_archived'] += 1
                        self.logger.info(f"✅ Successfully downloaded and archived blob: {blob_key}")
                    else:
                        error_msg = f"Failed to download blob with resume for {archived_build.build_id}"
                        results['errors'].append(error_msg)
                        self.logger.error(error_msg)
                    
        except Exception as e:
            error_msg = f"Failed to archive content for build {archived_build.build_id} (game {archived_build.game_id}): {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        # Save database after processing depot manifests
        self.save_database()
        
        return results
        
    def _archive_v2_depot_manifest_and_chunks(self, game_id: str, manifest_id: str, max_workers: int = 4) -> Dict:
        """Download and archive a v2 depot manifest and all its chunks"""
        result = {'success': False, 'chunks_archived': 0, 'errors': []}
        
        try:
            # Build depot manifest URL - v2 depot manifests are under /meta/ with galaxy_path structure
            galaxy_path = manifest_id
            if "/" not in galaxy_path:
                galaxy_path = manifest_id[0:2] + "/" + manifest_id[2:4] + "/" + manifest_id
            
            # Try multiple URL patterns for depot manifests
            depot_urls_to_try = [
                # New pattern (downloadable-manifests-collector)
                f"{constants.GOG_MANIFESTS_COLLECTOR}/manifests/depots/{galaxy_path}",
                # Old pattern (gog-cdn-fastly)
                f"{constants.GOG_CDN}/content-system/v2/meta/{galaxy_path}",
                # Alternative new pattern
                f"{constants.GOG_MANIFESTS_COLLECTOR}/depots/{galaxy_path}"
            ]
            
            raw_response = None
            depot_url = None
            
            # Try each URL until one works
            for url_to_try in depot_urls_to_try:
                print(f"DEBUG: Trying depot manifest URL: {url_to_try}")
                try:
                    test_response = self.api_handler.session.get(url_to_try)
                    if test_response.ok:
                        raw_response = test_response
                        depot_url = url_to_try
                        print(f"DEBUG: SUCCESS - Found depot manifest at: {depot_url}")
                        break
                    else:
                        print(f"DEBUG: FAILED - {url_to_try} returned {test_response.status_code}")
                except Exception as e:
                    print(f"DEBUG: ERROR - {url_to_try} failed with: {e}")
                    continue
            
            if not raw_response or not raw_response.ok:
                result['errors'].append(f"Failed to download depot manifest {manifest_id} from any URL pattern")
                return result
                
            raw_data = raw_response.content
            
            # Save raw depot manifest
            raw_path = self._save_raw_depot_manifest(depot_url, raw_data)
            
            # Decompress and parse depot manifest
            try:
                import gzip
                # Check for gzip first (starts with 0x1f 0x8b)
                if raw_data.startswith(b'\x1f\x8b'):
                    decompressed_data = gzip.decompress(raw_data)
                    depot_manifest = json.loads(decompressed_data.decode('utf-8'))
                # Check for zlib compression (starts with 0x78)
                elif raw_data.startswith(b'\x78'):
                    decompressed_data = zlib.decompress(raw_data, 15)
                    depot_manifest = json.loads(decompressed_data.decode('utf-8'))
                else:
                    # Try as plain JSON
                    depot_manifest = json.loads(raw_data.decode('utf-8'))
            except Exception as e:
                result['errors'].append(f"Failed to parse depot manifest {manifest_id}: {e}")
                return result
            
            # Save prettified depot manifest for human reading
            raw_path_obj = Path(raw_path)
            pretty_path = raw_path_obj.parent / f"{raw_path_obj.name}.json"
            with open(pretty_path, 'w') as f:
                json.dump(depot_manifest, f, indent=2)
                
            # Create archived manifest record
            chunks_referenced = set()
            file_count = 0
            total_size = 0
            
            depot_data = depot_manifest.get('depot', {})
            for file_record in depot_data.get('items', []):
                if file_record.get('type') == 'DepotFile':  # Only count files, not directories
                    file_count += 1
                    total_size += file_record.get('size', 0)
                    for chunk in file_record.get('chunks', []):
                        chunk_md5 = chunk.get('compressedMd5')  # Use compressedMd5 for URLs and storage
                        if chunk_md5:
                            chunks_referenced.add(chunk_md5)
            
            # Convert to relative path
            raw_path_obj = Path(raw_path)
            relative_path = raw_path_obj.relative_to(self.archive_root)
            
            archived_manifest = ArchivedManifest(
                manifest_id=manifest_id,
                game_id=game_id,
                version=2,
                manifest_type='depot',
                languages=['*'],  # TODO: Extract from build context
                archive_path=str(relative_path),
                cdn_url=depot_url,
                timestamp=time.time(),
                file_count=file_count,
                total_size=total_size,
                chunks_referenced=chunks_referenced
            )
            
            # Store in database
            self.archived_manifests[manifest_id] = archived_manifest
            
            # Download chunks if not in dry-run mode
            if chunks_referenced:
                self.logger.info(f"Found {len(chunks_referenced)} unique chunks to download")
                chunks_result = self._download_v2_chunks(game_id, chunks_referenced, max_workers)
                result['chunks_archived'] = chunks_result['chunks_archived']
                if chunks_result['errors']:
                    result['errors'].extend(chunks_result['errors'])
            
            result['success'] = True
            self.logger.info(f"Successfully archived depot manifest {manifest_id}: {file_count} files, {total_size} bytes, {len(chunks_referenced)} chunks")
            
        except Exception as e:
            error_msg = f"Failed to archive v2 depot manifest {manifest_id}: {e}"
            result['errors'].append(error_msg)
            self.logger.error(error_msg)
            
        return result
        
    def _download_v2_depot_manifest_only(self, game_id: str, manifest_id: str) -> Dict:
        """Download and archive a v2 depot manifest only - skip chunks"""
        result = {'success': False, 'errors': []}
        
        try:
            # Build depot manifest URL - v2 depot manifests are under /meta/ with galaxy_path structure
            galaxy_path = manifest_id
            if "/" not in galaxy_path:
                galaxy_path = manifest_id[0:2] + "/" + manifest_id[2:4] + "/" + manifest_id
            
            # Check if we already have this depot manifest on disk
            depot_path = self.archive_root / "manifests" / "v2" / "depots" / galaxy_path
            meta_path = self.archive_root / "manifests" / "v2" / "meta" / galaxy_path
            
            if depot_path.exists() or meta_path.exists():
                result['success'] = True
                result['already_exists'] = True
                return result
            
            # Try multiple URL patterns for depot manifests
            depot_urls_to_try = [
                # New pattern (downloadable-manifests-collector)
                f"{constants.GOG_MANIFESTS_COLLECTOR}/manifests/depots/{galaxy_path}",
                # Old pattern (gog-cdn-fastly)
                f"{constants.GOG_CDN}/content-system/v2/meta/{galaxy_path}",
                # Alternative new pattern
                f"{constants.GOG_MANIFESTS_COLLECTOR}/depots/{galaxy_path}"
            ]
            
            raw_response = None
            depot_url = None
            
            # Try each URL until one works
            for url_to_try in depot_urls_to_try:
                try:
                    test_response = self.api_handler.session.get(url_to_try)
                    if test_response.ok:
                        raw_response = test_response
                        depot_url = url_to_try
                        break
                except Exception as e:
                    continue
            
            if not raw_response or not raw_response.ok:
                result['errors'].append(f"Failed to download depot manifest {manifest_id} from any URL pattern")
                return result
                
            raw_data = raw_response.content
            
            # Save raw depot manifest
            raw_path = self._save_raw_depot_manifest(depot_url, raw_data)
            
            # Decompress and parse depot manifest
            try:
                import gzip
                # Check for gzip first (starts with 0x1f 0x8b)
                if raw_data.startswith(b'\x1f\x8b'):
                    decompressed_data = gzip.decompress(raw_data)
                    depot_manifest = json.loads(decompressed_data.decode('utf-8'))
                # Check for zlib compression (starts with 0x78)
                elif raw_data.startswith(b'\x78'):
                    decompressed_data = zlib.decompress(raw_data, 15)
                    depot_manifest = json.loads(decompressed_data.decode('utf-8'))
                else:
                    # Try as plain JSON
                    depot_manifest = json.loads(raw_data.decode('utf-8'))
            except Exception as e:
                result['errors'].append(f"Failed to parse depot manifest {manifest_id}: {e}")
                return result
            
            # Save prettified depot manifest for human reading
            raw_path_obj = Path(raw_path)
            pretty_path = raw_path_obj.parent / f"{raw_path_obj.name}.json"
            with open(pretty_path, 'w') as f:
                json.dump(depot_manifest, f, indent=2)
                
            # Create archived manifest record (collect chunk references but don't download them)
            chunks_referenced = set()
            file_count = 0
            total_size = 0
            
            depot_data = depot_manifest.get('depot', {})
            for file_record in depot_data.get('items', []):
                if file_record.get('type') == 'DepotFile':  # Only count files, not directories
                    file_count += 1
                    total_size += file_record.get('size', 0)
                    for chunk in file_record.get('chunks', []):
                        chunk_md5 = chunk.get('compressedMd5')  # Use compressedMd5 for URLs and storage
                        if chunk_md5:
                            chunks_referenced.add(chunk_md5)
            
            # Convert to relative path
            raw_path_obj = Path(raw_path)
            relative_path = raw_path_obj.relative_to(self.archive_root)
            
            archived_manifest = ArchivedManifest(
                manifest_id=manifest_id,
                game_id=game_id,
                version=2,
                manifest_type='depot',
                languages=['*'],  # TODO: Extract from build context
                archive_path=str(relative_path),
                cdn_url=depot_url,
                timestamp=time.time(),
                file_count=file_count,
                total_size=total_size,
                chunks_referenced=chunks_referenced
            )
            
            # Store in in-memory manifest tracking (not saved to database)
            self.archived_manifests[manifest_id] = archived_manifest
            
            result['success'] = True
            result['chunks_found'] = len(chunks_referenced)
            result['files_found'] = file_count
            result['success'] = True
            result['chunks_found'] = len(chunks_referenced)
            result['files_found'] = file_count
            result['total_size'] = total_size
            
        except Exception as e:
            result['errors'].append(f"Failed to download v2 depot manifest {manifest_id}: {e}")
            
        return result
        
    def _download_v1_depot_manifest_only(self, game_id: str, platform: str, build_id: str, repository_id: str, manifest_id: str) -> Dict:
        """Download and archive a v1 depot manifest only - skip blob"""
        result = {'success': False, 'errors': [], 'depot_manifest_data': None}
        
        try:
            # Build v1 depot manifest URL using correct API pattern
            # /content-system/v1/manifests/(product_id)/(os)/(repository_id)/(manifest_id)
            # Note: manifest_id already includes .json extension
            depot_url = f"{constants.GOG_CDN}/content-system/v1/manifests/{game_id}/{platform}/{repository_id}/{manifest_id}"
            
            self.logger.info(f"Downloading v1 depot manifest from: {depot_url}")
            
            # Check if we already have this manifest
            if manifest_id in self.archived_manifests:
                self.logger.info(f"V1 depot manifest {manifest_id} already archived")
                result['success'] = True
                result['already_exists'] = True
                return result
            
            # Download depot manifest  
            # from gogdl.dl import dl_utils
            depot_manifest = dl_utils.get_json(self.api_handler, depot_url)
            if not depot_manifest:
                result['errors'].append(f"Failed to download v1 depot manifest {manifest_id}")
                return result
            
            self.logger.info(f"Successfully downloaded V1 depot manifest {manifest_id}, size: {len(str(depot_manifest))} chars")
            
            # Save raw depot manifest (as JSON since v1 isn't compressed)
            raw_data = json.dumps(depot_manifest).encode('utf-8')
            self.logger.info(f"About to save raw depot manifest to archive, raw_data size: {len(raw_data)} bytes")
            raw_path = self._save_raw_depot_manifest(depot_url, raw_data)
            self.logger.info(f"Raw depot manifest saved to: {raw_path}")
            
            # Create ArchivedManifest record for database
            archived_manifest = ArchivedManifest(
                manifest_id=manifest_id,
                game_id=game_id,
                version=1,
                manifest_type="depot",
                languages=["English"],  # Default, could be extracted from depot if needed
                archive_path=str(Path(raw_path).relative_to(self.archive_root)),
                cdn_url=depot_url,
                timestamp=time.time(),
                file_count=len(depot_manifest.get("depot", {}).get("files", [])),
                total_size=sum(f.get("size", 0) for f in depot_manifest.get("depot", {}).get("files", []) if "directory" not in f),
                chunks_referenced=set()  # v1 depot manifests don't reference chunks
            )
            
            # DO NOT store depot manifests in database - only download and save to disk
            
            result['success'] = True
            result['depot_manifest_data'] = depot_manifest  # Return the manifest data for deduplication
            print(f"✅ Downloaded v1 depot manifest {manifest_id}: {archived_manifest.file_count} files, {archived_manifest.total_size:,} bytes (blob not downloaded)")
            print(f"   📁 Saved to: {raw_path}")
            print(f"   🌐 Full URL: {depot_url}")
            
        except Exception as e:
            result['errors'].append(f"Failed to download v1 depot manifest {manifest_id}: {e}")
            self.logger.error(f"Failed to download v1 depot manifest {manifest_id}: {e}")
            
        return result
        
    # def _archive_v1_depot_manifest_and_blob(self, game_id: str, platform: str, build_id: str, repository_id: str, manifest_id: str, max_workers: int = 4) -> Dict:
    #     """Download and archive a v1 depot manifest and its main.bin blob"""
    #     result = {'success': False, 'blobs_archived': 0, 'errors': []}
        
    #     try:
    #         # Build v1 depot manifest URL using correct API pattern
    #         # /content-system/v1/manifests/(product_id)/(os)/(repository_id)/(manifest_id)
    #         # Note: manifest_id already includes .json extension
    #         depot_url = f"{constants.GOG_CDN}/content-system/v1/manifests/{game_id}/{platform}/{repository_id}/{manifest_id}"
            
    #         self.logger.info(f"Downloading v1 depot manifest from: {depot_url}")
            
    #         # Check if we already have this manifest
    #         if manifest_id in self.archived_manifests:
    #             self.logger.info(f"V1 depot manifest {manifest_id} already archived")
    #             result['success'] = True
    #             return result
            
    #         # Download depot manifest  
    #         depot_manifest = dl_utils.get_json(self.api_handler, depot_url)
    #         if not depot_manifest:
    #             result['errors'].append(f"Failed to download v1 depot manifest {manifest_id}")
    #             return result
            
    #         self.logger.info(f"Successfully downloaded V1 depot manifest {manifest_id}, size: {len(str(depot_manifest))} chars")
            
    #         # Save raw depot manifest (as JSON since v1 isn't compressed)
    #         raw_data = json.dumps(depot_manifest).encode('utf-8')
    #         self.logger.info(f"About to save raw depot manifest to archive, raw_data size: {len(raw_data)} bytes")
    #         raw_path = self._save_raw_depot_manifest(depot_url, raw_data)
    #         self.logger.info(f"Raw depot manifest saved to: {raw_path}")
            
    #         # Create ArchivedManifest record for database
    #         archived_manifest = ArchivedManifest(
    #             manifest_id=manifest_id,
    #             game_id=game_id,
    #             version=1,
    #             manifest_type="depot",
    #             languages=["English"],  # Default, could be extracted from depot if needed
    #             archive_path=str(Path(raw_path).relative_to(self.archive_root)),
    #             cdn_url=depot_url,
    #             timestamp=time.time(),
    #             file_count=len(depot_manifest.get("depot", {}).get("files", [])),
    #             total_size=sum(f.get("size", 0) for f in depot_manifest.get("depot", {}).get("files", []) if "directory" not in f),
    #             chunks_referenced=set()  # v1 depot manifests don't reference chunks
    #         )
            
    #         # Store in database
    #         self.archived_manifests[manifest_id] = archived_manifest
            
    #         # Extract file information and find the blob URL
    #         files_in_depot = []
    #         total_blob_size = 0
    #         blob_url_from_manifest = None
            
    #         for record in depot_manifest.get("depot", {}).get("files", []):
    #             if "directory" not in record:  # Skip directories
    #                 # Extract blob URL from first file that has a URL (they should all be the same)
    #                 if blob_url_from_manifest is None and record.get("url"):
    #                     blob_url_from_manifest = record.get("url", "")
                        
    #                 file_info = {
    #                     'path': record["path"].lstrip("/"),
    #                     'offset': record.get("offset", 0),
    #                     'size': record["size"],
    #                     'hash': record.get("hash", "")
    #                 }
    #                 files_in_depot.append(file_info)
    #                 # Calculate total size needed (max offset + size)
    #                 end_pos = file_info['offset'] + file_info['size']
    #                 total_blob_size = max(total_blob_size, end_pos)
            
    #         if not blob_url_from_manifest:
    #             result['errors'].append(f"No blob URL found in depot manifest {manifest_id}")
    #             return result
                
    #         self.logger.info(f"Found blob URL in manifest: {blob_url_from_manifest}")
            
    #         # Update the archived manifest to include the build_id reference for consistency
    #         archived_manifest.chunks_referenced = {build_id}  # Store build_id for consistent tracking with storage
            
    #         # Check if we already have this blob - use file system truth instead of database lookup
    #         blob_file_path = self.blobs_dir / build_id / "main.bin"
            
    #         if blob_file_path.exists():
    #             actual_size = blob_file_path.stat().st_size
    
    #             # Get authoritative expected size from server via HEAD request
    #             try:
    #                 # Build the blob URL for HEAD request (same as download logic)
    #                 secure_links = dl_utils.get_secure_link(
    #                     self.api_handler, f"/{platform}/{repository_id}/", game_id, generation=1
    #                 )
        
    #                 if isinstance(secure_links, str):
    #                     blob_url = f"{secure_links}/main.bin"
    #                 else:
    #                     endpoint = secure_links[0].copy()
    #                     endpoint["parameters"]["path"] += "/main.bin"
    #                     blob_url = dl_utils.merge_url_with_params(
    #                         endpoint["url_format"], endpoint["parameters"]
    #                     )
        
    #                 # Perform HEAD request to get Content-Length (authoritative size)
    #                 head_response = self.api_handler.session.head(blob_url, timeout=30)
    #                 expected_size_from_server = int(head_response.headers.get('Content-Length', 0))
            
    #                 # Validate size matches server's Content-Length (authoritative)
    #                 if expected_size_from_server > 0 and actual_size != expected_size_from_server:
    #                     self.logger.warning(f"⚠️  Blob size mismatch for {build_id}/main.bin:")
    #                     self.logger.warning(f"  File: {blob_file_path}")
    #                     self.logger.warning(f"  Actual size: {actual_size:,} bytes ({actual_size / (1024**3):.2f} GB)")
    #                     self.logger.warning(f"  Server size: {expected_size_from_server:,} bytes ({expected_size_from_server / (1024**3):.2f} GB)")
    #                     self.logger.info(f"  Re-downloading blob due to size mismatch")
    #                     # Continue to download section by not returning here
    #                 elif actual_size == 0:
    #                     self.logger.warning(f"⚠️  Blob {build_id}/main.bin is empty (0 bytes):")
    #                     self.logger.warning(f"  File: {blob_file_path}")
    #                     self.logger.warning(f"  Server size: {expected_size_from_server:,} bytes ({expected_size_from_server / (1024**3):.2f} GB)")
    #                     self.logger.info(f"  Re-downloading blob due to empty file")
    #                     # Continue to download section by not returning here
    #                 else:
    #                     # Size is correct, proceed with skip logic
    #                     self.logger.info(f"⚡ Blob {build_id}/main.bin already exists:")
    #                     self.logger.info(f"  File: {blob_file_path}")
    #                     self.logger.info(f"  Size: {actual_size:,} bytes ({actual_size / (1024**3):.2f} GB)")
    #                     self.logger.info(f"  Server: {expected_size_from_server:,} bytes - ✅ Size matches!")
    #                     self.logger.info(f"  Skipping download to avoid re-download")
    #                     # Handle database record for existing valid blob
    #                     blob_key = f"{game_id}_{build_id}_main.bin"
    #                     if blob_key in self.archived_blobs:
    #                         existing_blob = self.archived_blobs[blob_key]
                            
    #                         # Add this manifest to the list of referencing manifests
    #                         if existing_blob.depot_info and 'referenced_by_manifests' in existing_blob.depot_info:
    #                             if manifest_id not in existing_blob.depot_info['referenced_by_manifests']:
    #                                 existing_blob.depot_info['referenced_by_manifests'].append(manifest_id)
    #                                 self.logger.info(f"  Added manifest {manifest_id} to blob references")
    #                         else:
    #                             # Handle legacy format or missing depot_info
    #                             if not existing_blob.depot_info:
    #                                 existing_blob.depot_info = {}
    #                             existing_blob.depot_info['referenced_by_manifests'] = [manifest_id]
    #                             self.logger.info(f"  Initialized blob references with manifest {manifest_id}")
                            
    #                         # Update last_verified timestamp
    #                         existing_blob.last_verified = time.time()
    #                     else:
    #                         # Create database record for existing file
    #                         archived_blob = ArchivedBlob(
    #                             depot_manifest=blob_key,
    #                             secure_url="",
    #                             total_size=actual_size,
    #                             archive_path=str(blob_file_path),
    #                             first_seen=time.time(),
    #                             last_verified=time.time(),
    #                             files_contained=[],
    #                             depot_info={
    #                                 'game_id': game_id,
    #                                 'platform': platform,
    #                                 'build_id': build_id,
    #                                 'actual_size': actual_size,
    #                                 'referenced_by_manifests': [manifest_id]
    #                             }
    #                         )
    #                         self.archived_blobs[blob_key] = archived_blob
    #                         self.logger.info(f"  Created database record for existing blob")
                        
    #                     # Save database to record the new manifest reference
    #                     self.save_database()
    #                     result['success'] = True
    #                     return result
            
    #             except Exception as e:
    #                 self.logger.warning(f"⚠️  Could not verify blob size with server (HEAD request failed): {e}")
    #                 self.logger.warning(f"  Using manifest size for comparison: {total_blob_size:,} bytes")
        
    #                 # Fallback to manifest size comparison
    #                 if total_blob_size > 0 and actual_size != total_blob_size:
    #                     self.logger.warning(f"  Actual size: {actual_size:,} bytes, Manifest size: {total_blob_size:,} bytes")
    #                     self.logger.info(f"  Re-downloading blob due to size mismatch")
    #                     # Continue to download section
    #                 elif actual_size == 0:
    #                     self.logger.warning(f"  File is empty (0 bytes), expected: {total_blob_size:,} bytes")
    #                     self.logger.info(f"  Re-downloading blob due to empty file")
    #                     # Continue to download section
    #                 else:
    #                     # Size matches manifest, proceed with skip
    #                     self.logger.info(f"⚡ Blob {build_id}/main.bin already exists:")
    #                     self.logger.info(f"  File: {blob_file_path}")
    #                     self.logger.info(f"  Size: {actual_size:,} bytes ({actual_size / (1024**3):.2f} GB)")
    #                     self.logger.info(f"  Manifest: {total_blob_size:,} bytes - ✅ Size matches!")
    #                     self.logger.info(f"  Skipping download to avoid re-download")
    #                     # Handle database record for existing valid blob
    #                     blob_key = f"{game_id}_{build_id}_main.bin"
    #                     if blob_key in self.archived_blobs:
    #                         existing_blob = self.archived_blobs[blob_key]
                            
    #                         # Add this manifest to the list of referencing manifests
    #                         if existing_blob.depot_info and 'referenced_by_manifests' in existing_blob.depot_info:
    #                             if manifest_id not in existing_blob.depot_info['referenced_by_manifests']:
    #                                 existing_blob.depot_info['referenced_by_manifests'].append(manifest_id)
    #                                 self.logger.info(f"  Added manifest {manifest_id} to blob references")
    #                         else:
    #                             # Handle legacy format or missing depot_info
    #                             if not existing_blob.depot_info:
    #                                 existing_blob.depot_info = {}
    #                             existing_blob.depot_info['referenced_by_manifests'] = [manifest_id]
    #                             self.logger.info(f"  Initialized blob references with manifest {manifest_id}")
                            
    #                         # Update last_verified timestamp
    #                         existing_blob.last_verified = time.time()
    #                     else:
    #                         # Create database record for existing file
    #                         archived_blob = ArchivedBlob(
    #                             depot_manifest=blob_key,
    #                             secure_url="",
    #                             total_size=actual_size,
    #                             archive_path=str(blob_file_path),
    #                             first_seen=time.time(),
    #                             last_verified=time.time(),
    #                             files_contained=[],
    #                             depot_info={
    #                                 'game_id': game_id,
    #                                 'platform': platform,
    #                                 'build_id': build_id,
    #                                 'actual_size': actual_size,
    #                                 'referenced_by_manifests': [manifest_id]
    #                             }
    #                         )
    #                         self.archived_blobs[blob_key] = archived_blob
    #                         self.logger.info(f"  Created database record for existing blob")
                        
    #                     # Save database to record the new manifest reference
    #                     self.save_database()
    #                     result['success'] = True
    #                     return result
            
    #         self.logger.info(f"Downloading v1 blob {blob_url_from_manifest} (estimated size: {total_blob_size} bytes, {len(files_in_depot)} files)")
            
    #         # Use resume-capable download instead of streaming download
    #         blob_path = self.blobs_dir / build_id / "main.bin"
            
    #         # Get expected size from server via HEAD request first
    #         secure_links = dl_utils.get_secure_link(
    #             self.api_handler, f"/{platform}/{repository_id}/", game_id, generation=1
    #         )
            
    #         if isinstance(secure_links, str):
    #             blob_url = f"{secure_links}/main.bin"
    #         else:
    #             endpoint = secure_links[0].copy()
    #             endpoint["parameters"]["path"] += "/main.bin"
    #             blob_url = dl_utils.merge_url_with_params(
    #                 endpoint["url_format"], endpoint["parameters"]
    #             )
            
    #         # Get expected size from HEAD request
    #         head_response = self.api_handler.session.head(blob_url, timeout=30)
    #         expected_size = int(head_response.headers.get('Content-Length', 0))
    #         self.logger.info(f"Expected blob size from server: {expected_size:,} bytes ({expected_size / (1024**3):.2f} GB)")
            
    #         # Use block-based resume download
    #         if not self._download_v1_blob_with_resume(game_id, platform, repository_id, build_id, blob_path, expected_size):
    #             result['errors'].append(f"Failed to download blob with resume for {build_id}")
    #             return result

    #         # Generate checksums after successful download
    #         self.logger.info("📋 Generating checksums for downloaded blob...")
    #         if self._generate_blob_checksum_xml(blob_path, expected_size):
    #             self.logger.info("✅ Checksum generation complete")
    #         else:
    #             self.logger.warning("⚠️  Checksum generation failed (download still successful)")

    #         actual_size = blob_path.stat().st_size
            
    #         # Create consistent key for blob tracking
    #         blob_key = f"{game_id}_{build_id}_main.bin"
            
    #         # Create archived blob record (use consistent key)
    #         archived_blob = ArchivedBlob(
    #             depot_manifest=blob_key,  # Use consistent key instead of blob URL
    #             secure_url=blob_url,  # Keep full URL including /main.bin
    #             total_size=actual_size,
    #             archive_path=str(blob_path),
    #             first_seen=time.time(),
    #             last_verified=time.time(),
    #             files_contained=[],  # Remove redundant data - use manifests instead
    #             depot_info={
    #                 'game_id': game_id,
    #                 'platform': platform,
    #                 'build_id': build_id,
    #                 'estimated_size': total_blob_size,
    #                 'actual_size': actual_size,
    #                 'referenced_by_manifests': [manifest_id]  # List to support multiple manifests
    #             }
    #         )
            
    #         # Store in database using consistent key
    #         self.archived_blobs[blob_key] = archived_blob
            
    #         self.logger.info(f"Archived v1 blob: {blob_key} at {blob_path} ({actual_size} bytes, {len(files_in_depot)} files)")
            
    #         # Save database with both manifest and blob records
    #         self.save_database()
            
    #         result['blobs_archived'] = 1
    #         result['success'] = True
            
    #     except Exception as e:
    #         result['errors'].append(f"Failed to archive v1 depot manifest {manifest_id}: {e}")
    #         self.logger.error(f"Failed to archive v1 depot manifest {manifest_id}: {e}")
            
    #     return result

    def _generate_blob_checksum_xml(self, blob_path: Path, expected_size: int) -> bool:
        """Generate checksum files (both XML and JSON) for a blob with 100 MiB chunks"""
        from datetime import datetime

        # Both XML and JSON file paths - for compatibility/migration
        xml_path = blob_path.with_suffix('.xml')
        json_path = blob_path.with_suffix('.json')
        chunk_size = 100 * 1024**2  # 100 MiB chunks (lgogdownloader standard)

        try:
            # Calculate total chunks needed
            total_chunks = (expected_size + chunk_size - 1) // chunk_size
            
            self.logger.info(f"🧮 Generating checksums for {blob_path.name}: {total_chunks} chunks of 100 MiB each")
            
            # Open blob file for reading
            with open(blob_path, 'rb') as f:
                chunks_data = []
                overall_md5 = hashlib.md5()
                overall_sha1 = hashlib.sha1()
                overall_sha256 = hashlib.sha256()
                
                for chunk_id in range(total_chunks):
                    chunk_start = chunk_id * chunk_size
                    chunk_end = min(chunk_start + chunk_size - 1, expected_size - 1)
                    actual_chunk_size = chunk_end - chunk_start + 1
                    
                    # Read chunk data
                    f.seek(chunk_start)
                    chunk_data = f.read(actual_chunk_size)
                    
                    if len(chunk_data) != actual_chunk_size:
                        self.logger.error(f"Failed to read chunk {chunk_id}: got {len(chunk_data)} bytes, expected {actual_chunk_size}")
                        return False
                    
                    # Calculate chunk checksums (all three types)
                    chunk_md5 = hashlib.md5(chunk_data).hexdigest()
                    chunk_sha1 = hashlib.sha1(chunk_data).hexdigest()
                    chunk_sha256 = hashlib.sha256(chunk_data).hexdigest()
                    
                    # Update overall checksums
                    overall_md5.update(chunk_data)
                    overall_sha1.update(chunk_data)
                    overall_sha256.update(chunk_data)
                    
                    chunks_data.append({
                        'id': chunk_id,
                        'from': chunk_start,
                        'to': chunk_end,
                        'md5': chunk_md5,
                        'sha1': chunk_sha1,
                        'sha256': chunk_sha256
                    })
                    
                    # Progress logging
                    if (chunk_id + 1) % 10 == 0 or chunk_id == total_chunks - 1:
                        progress = ((chunk_id + 1) / total_chunks) * 100
                        self.logger.info(f"   📊 Checksum progress: {chunk_id + 1}/{total_chunks} chunks ({progress:.1f}%)")
            
            # Generate overall hash values
            overall_md5_hex = overall_md5.hexdigest()
            overall_sha1_hex = overall_sha1.hexdigest()
            overall_sha256_hex = overall_sha256.hexdigest()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            iso_timestamp = datetime.now().isoformat()
            
            # Generate XML content (for lgogdownloader compatibility)
            xml_content = f'<file name="{blob_path.name}" available="1" notavailablemsg="" md5="{overall_md5_hex}" sha1="{overall_sha1_hex}" sha256="{overall_sha256_hex}" chunks="{total_chunks}" timestamp="{timestamp}" total_size="{expected_size}">\n'
            
            for chunk in chunks_data:
                # Single line with all hash methods as attributes (compact format)
                xml_content += f'\t<chunk id="{chunk["id"]}" from="{chunk["from"]}" to="{chunk["to"]}" md5="{chunk["md5"]}" sha1="{chunk["sha1"]}" sha256="{chunk["sha256"]}" />\n'
            
            xml_content += '</file>\n'
            
            # Write XML file
            with open(xml_path, 'w', encoding='utf-8') as f:
                f.write(xml_content)
            
            # Generate JSON content (for improved parsing and future use)
            json_data = {
                "file_name": blob_path.name,
                "available": True,
                "total_size": expected_size,
                "total_chunks": total_chunks,
                "completed_chunks": total_chunks,
                "timestamp": iso_timestamp,
                "overall_hashes": {
                    "md5": overall_md5_hex,
                    "sha1": overall_sha1_hex,
                    "sha256": overall_sha256_hex
                },
                "completed_chunk_ids": [chunk["id"] for chunk in chunks_data],
                "chunk_hashes": {}
            }
            
            # Add chunk hash data
            for chunk in chunks_data:
                json_data["chunk_hashes"][str(chunk["id"])] = {
                    "from": chunk["from"],
                    "to": chunk["to"],
                    "md5": chunk["md5"],
                    "sha1": chunk["sha1"],
                    "sha256": chunk["sha256"]
                }
            
            # Write JSON file
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, sort_keys=True)
            
            self.logger.info(f"✅ Generated checksum files:")
            self.logger.info(f"   📋 XML: {xml_path}")
            self.logger.info(f"   📋 JSON: {json_path}")
            self.logger.info(f"   📋 File MD5: {overall_md5_hex}")
            self.logger.info(f"   📋 File SHA1: {overall_sha1_hex}")
            self.logger.info(f"   📋 File SHA256: {overall_sha256_hex}")
            self.logger.info(f"   📦 Chunks: {total_chunks}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to generate checksum files for {blob_path}: {e}")
            return False

    def _download_v1_blob_with_resume(self, game_id: str, platform: str, repository_id: str, 
                                  build_id: str, blob_path: Path, expected_size: int) -> bool:
        """Download v1 blob with block-based resume capability"""
        from datetime import datetime

        chunk_size = 100 * 1024 * 1024  # 100 MiB chunks
        json_path = blob_path.with_suffix('.json')
    
        try:
            # Get secure links for download
            secure_links = dl_utils.get_secure_link(
                self.api_handler, f"/{platform}/{repository_id}/", game_id, generation=1
            )
        
            if isinstance(secure_links, str):
                blob_url = f"{secure_links}/main.bin"
            else:
                endpoint = secure_links[0].copy()
                endpoint["parameters"]["path"] += "/main.bin"
                blob_url = dl_utils.merge_url_with_params(
                    endpoint["url_format"], endpoint["parameters"]
                )
        
            # Calculate chunks needed
            total_chunks = (expected_size + chunk_size - 1) // chunk_size
            self.logger.info(f"🚀 Starting block-based download: {total_chunks} chunks of 100 MiB")
        
            # Check existing file and JSON metadata
            chunks_to_download = []
            existing_chunks = {}
        
            if blob_path.exists() and json_path.exists():
                self.logger.info("📋 Found existing file and checksums, validating...")
                existing_chunks = self._parse_existing_checksum_json(json_path)
            
                # Parse JSON to get both chunk hashes and validation states
                existing_chunk_states = {}
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    existing_chunk_states = json_data.get('chunk_states', {})
                except Exception as e:
                    self.logger.warning(f"Failed to parse chunk states from JSON: {e}")
                
                # Validate existing chunks intelligently based on their validation state
                with open(blob_path, 'rb') as f:
                    for chunk_id in range(total_chunks):
                        chunk_start = chunk_id * chunk_size
                        chunk_end = min(chunk_start + chunk_size - 1, expected_size - 1)
                        actual_chunk_size = chunk_end - chunk_start + 1
                    
                        if chunk_id in existing_chunks:
                            chunk_id_str = str(chunk_id)
                            
                            # Check if chunk is already marked as validated in JSON state
                            if (chunk_id_str in existing_chunk_states and 
                                existing_chunk_states[chunk_id_str].get('status') == 'validated'):
                                # Chunk already validated, trust the JSON state - no need to re-validate
                                self.logger.debug(f"   ✅ Chunk {chunk_id} already validated, skipping")
                                continue
                            
                            # Chunk exists but not validated - perform validation
                            self.logger.info(f"   🔍 Validating chunk {chunk_id}...")
                            f.seek(chunk_start)
                            chunk_data = f.read(actual_chunk_size)
                            
                            # Skip validation if chunk appears to be zero-filled (incomplete download)
                            if chunk_data == b'\x00' * actual_chunk_size:
                                self.logger.warning(f"   ⚠️  Chunk {chunk_id} appears zero-filled, will re-download")
                                chunks_to_download.append(chunk_id)
                                continue
                            
                            # Try to validate with available hash methods (prefer strongest)
                            chunk_valid = False
                            if 'sha256' in existing_chunks[chunk_id]:
                                chunk_sha256 = hashlib.sha256(chunk_data).hexdigest()
                                if chunk_sha256 == existing_chunks[chunk_id]['sha256']:
                                    chunk_valid = True
                            elif 'sha1' in existing_chunks[chunk_id]:
                                chunk_sha1 = hashlib.sha1(chunk_data).hexdigest()
                                if chunk_sha1 == existing_chunks[chunk_id]['sha1']:
                                    chunk_valid = True
                            elif 'md5' in existing_chunks[chunk_id]:
                                chunk_md5 = hashlib.md5(chunk_data).hexdigest()
                                if chunk_md5 == existing_chunks[chunk_id]['md5']:
                                    chunk_valid = True
                            
                            if chunk_valid:
                                # Mark as validated with current timestamp, preserve existing download_time
                                validation_time = datetime.now().isoformat()
                                if chunk_id_str not in existing_chunk_states:
                                    existing_chunk_states[chunk_id_str] = {}
                                
                                # Preserve existing download_time if it exists
                                existing_download_time = existing_chunk_states[chunk_id_str].get('download_time')
                                
                                existing_chunk_states[chunk_id_str].update({
                                    'status': 'validated',
                                    'validation_time': validation_time
                                })
                                
                                # Keep existing download_time or set to validation_time if new
                                if existing_download_time:
                                    existing_chunk_states[chunk_id_str]['download_time'] = existing_download_time
                                else:
                                    existing_chunk_states[chunk_id_str]['download_time'] = validation_time
                                
                                self.logger.debug(f"   ✅ Chunk {chunk_id} validated successfully")
                                continue  # Chunk is valid, skip download
                            else:
                                self.logger.warning(f"   ⚠️  Chunk {chunk_id} corrupted, will re-download")
                        
                        chunks_to_download.append(chunk_id)
            else:
                # No existing file or metadata, download all chunks
                chunks_to_download = list(range(total_chunks))
            
                # Create directory but DON'T pre-allocate file to avoid false resume validation
                blob_path.parent.mkdir(parents=True, exist_ok=True)
                # NOTE: File will be created during first chunk write
        
            self.logger.info(f"📥 Chunks to download: {len(chunks_to_download)} of {total_chunks}")
        
            if not chunks_to_download:
                self.logger.info("✅ All chunks validated, download complete!")
                return True
        
            # Initialize incremental overall hash objects for efficient updates
            overall_md5 = hashlib.md5()
            overall_sha1 = hashlib.sha1()
            overall_sha256 = hashlib.sha256()
            
            # Pre-populate overall hashes with existing validated chunks
            if existing_chunks and blob_path.exists():
                self.logger.info("🔄 Pre-loading overall hashes from existing chunks...")
                with open(blob_path, 'rb') as f:
                    for chunk_id in sorted(existing_chunks.keys()):
                        chunk_start = existing_chunks[chunk_id]['from']
                        chunk_end = existing_chunks[chunk_id]['to']
                        actual_chunk_size = chunk_end - chunk_start + 1
                        
                        f.seek(chunk_start)
                        chunk_data = f.read(actual_chunk_size)
                        
                        # Add existing chunk to running overall hashes
                        overall_md5.update(chunk_data)
                        overall_sha1.update(chunk_data)
                        overall_sha256.update(chunk_data)
        
            # Download missing/corrupted chunks with incremental JSON updates
            for i, chunk_id in enumerate(chunks_to_download):
                chunk_start = chunk_id * chunk_size
                chunk_end = min(chunk_start + chunk_size - 1, expected_size - 1)
                actual_chunk_size = chunk_end - chunk_start + 1
            
                self.logger.info(f"📥 [{i+1}/{len(chunks_to_download)}] Downloading chunk {chunk_id} ({chunk_start}-{chunk_end}, {actual_chunk_size:,} bytes)")
            
                try:
                    # HTTP Range request for this chunk
                    headers = {'Range': f'bytes={chunk_start}-{chunk_end}'}
                    response = self.api_handler.session.get(blob_url, headers=headers, stream=True, timeout=(30, 300))
                
                    if response.status_code not in (206, 200):  # 206 = Partial Content, 200 = OK (full file)
                        self.logger.error(f"Range request failed for chunk {chunk_id}: HTTP {response.status_code}")
                        # Update JSON with current progress before failing
                        self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks)
                        return False
                
                    # Read chunk data
                    chunk_data = b''
                    for data in response.iter_content(chunk_size=65536):
                        chunk_data += data
                
                    if len(chunk_data) != actual_chunk_size:
                        self.logger.error(f"Chunk {chunk_id} size mismatch: got {len(chunk_data)}, expected {actual_chunk_size}")
                        # Update JSON with current progress before failing
                        self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks)
                        return False
                
                    # Calculate multi-hash checksums for the chunk
                    chunk_md5 = hashlib.md5(chunk_data).hexdigest()
                    chunk_sha1 = hashlib.sha1(chunk_data).hexdigest()
                    chunk_sha256 = hashlib.sha256(chunk_data).hexdigest()
                
                    # Write chunk to file - smart file creation
                    # Only allocate file space when we actually have data to write
                    if not blob_path.exists():
                        blob_path.parent.mkdir(parents=True, exist_ok=True)
                        # Create empty file initially - we'll write chunks as we get them
                        blob_path.touch()
                    
                    # Expand file if needed to accommodate this chunk
                    current_size = blob_path.stat().st_size if blob_path.exists() else 0
                    required_size = chunk_end + 1
                    
                    if current_size < required_size:
                        # Extend file to accommodate this chunk (without zero-filling gaps)
                        with open(blob_path, 'r+b') as f:
                            f.seek(required_size - 1)
                            f.write(b'\0')
                    
                    # Write chunk data at correct position
                    with open(blob_path, 'r+b') as f:
                        f.seek(chunk_start)
                        f.write(chunk_data)
                
                    # Update existing_chunks with new chunk data
                    existing_chunks[chunk_id] = {
                        'from': chunk_start,
                        'to': chunk_end,
                        'md5': chunk_md5,
                        'sha1': chunk_sha1,
                        'sha256': chunk_sha256
                    }
                
                    # Incrementally update overall hashes with this new chunk
                    # This is O(1) instead of O(n) - much more efficient!
                    overall_md5.update(chunk_data)
                    overall_sha1.update(chunk_data)
                    overall_sha256.update(chunk_data)
                
                    # Create incremental hash objects for JSON update
                    # Use copy() to preserve state for next iteration
                    incremental_hashes = {
                        'md5': overall_md5.copy(),
                        'sha1': overall_sha1.copy(),
                        'sha256': overall_sha256.copy()
                    }
                
                    # Incrementally update JSON file after each successful chunk
                    # Pass incremental hashes to avoid re-reading entire file
                    # Pass the chunk that was just downloaded to get a new timestamp
                    self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks, 
                                                         existing_chunks, incremental_hashes, {chunk_id})
                
                    # Progress logging
                    progress = ((i + 1) / len(chunks_to_download)) * 100
                    self.logger.info(f"   ✅ Chunk {chunk_id} complete ({progress:.1f}%) - JSON updated")
                    
                except KeyboardInterrupt:
                    self.logger.warning("⚠️  Download interrupted by user")
                    # Update JSON with current progress before exiting - use incremental hashes if available
                    if 'incremental_hashes' in locals():
                        self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks, 
                                                             existing_chunks, incremental_hashes, None)
                    else:
                        self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks, existing_chunks, None, None)
                    self.logger.info("📋 JSON metadata saved with current progress")
                    raise  # Re-raise to maintain interrupt behavior
                except Exception as e:
                    self.logger.error(f"Failed to download chunk {chunk_id}: {e}")
                    # Update JSON with current progress before continuing/failing
                    if 'incremental_hashes' in locals():
                        self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks, 
                                                             existing_chunks, incremental_hashes, None)
                    else:
                        self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks, existing_chunks, None, None)
                    return False
        
            # Final JSON validation and completion
            self.logger.info("📋 Performing final JSON validation...")
            final_incremental_hashes = {
                'md5': overall_md5.copy(),
                'sha1': overall_sha1.copy(),
                'sha256': overall_sha256.copy()
            }
            final_json_success = self._update_json_with_current_chunks(json_path, blob_path, expected_size, total_chunks, 
                                                                      existing_chunks, final_incremental_hashes, None)
            if not final_json_success:
                self.logger.warning("⚠️  Final JSON validation failed (download still successful)")
        
            self.logger.info("🎉 Block-based download completed successfully!")
            return True
        
        except Exception as e:
            self.logger.error(f"Block-based download failed: {e}")
            return False

    def _update_json_with_current_chunks(self, json_path: Path, blob_path: Path, expected_size: int, 
                                        total_chunks: int, current_chunks: dict = None, 
                                        incremental_overall_hashes: dict = None, 
                                        newly_processed_chunks: set = None) -> bool:
        """Update JSON file with current chunk status - safe for interruptions
        
        Args:
            incremental_overall_hashes: Optional dict with 'md5', 'sha1', 'sha256' hash objects
                                      for incremental updates instead of re-reading file
            newly_processed_chunks: Set of chunk IDs that were just downloaded/validated and need new timestamps
        """
        from datetime import datetime
        
        chunk_size = 100 * 1024**2  # 100 MiB chunks
        
        try:
            # If no chunks provided, scan the file to determine current state
            if current_chunks is None:
                current_chunks = {}
                
                # Parse existing JSON if available
                if json_path.exists():
                    current_chunks = self._parse_existing_checksum_json(json_path)
                
                # Validate chunks against actual file content
                if blob_path.exists():
                    with open(blob_path, 'rb') as f:
                        for chunk_id in range(total_chunks):
                            chunk_start = chunk_id * chunk_size
                            chunk_end = min(chunk_start + chunk_size - 1, expected_size - 1)
                            actual_chunk_size = chunk_end - chunk_start + 1
                            
                            # Read chunk data
                            f.seek(chunk_start)
                            chunk_data = f.read(actual_chunk_size)
                            
                            # Only calculate hashes for chunks that have the right size
                            if len(chunk_data) == actual_chunk_size and chunk_data != b'\x00' * actual_chunk_size:
                                # Calculate all three hash types
                                chunk_md5 = hashlib.md5(chunk_data).hexdigest()
                                chunk_sha1 = hashlib.sha1(chunk_data).hexdigest()
                                chunk_sha256 = hashlib.sha256(chunk_data).hexdigest()
                                
                                current_chunks[chunk_id] = {
                                    'from': chunk_start,
                                    'to': chunk_end,
                                    'md5': chunk_md5,
                                    'sha1': chunk_sha1,
                                    'sha256': chunk_sha256
                                }
            
            # Calculate overall file hashes - use incremental approach if provided
            if incremental_overall_hashes:
                # Use provided incremental hash objects (most efficient)
                overall_md5_hex = incremental_overall_hashes['md5'].hexdigest()
                overall_sha1_hex = incremental_overall_hashes['sha1'].hexdigest()
                overall_sha256_hex = incremental_overall_hashes['sha256'].hexdigest()
            else:
                # Fallback: re-calculate from existing chunks (less efficient but safe)
                overall_md5 = hashlib.md5()
                overall_sha1 = hashlib.sha1()
                overall_sha256 = hashlib.sha256()
                
                # Read file in chunk order and update overall hashes
                if blob_path.exists():
                    with open(blob_path, 'rb') as f:
                        for chunk_id in sorted(current_chunks.keys()):
                            chunk_start = current_chunks[chunk_id]['from']
                            chunk_end = current_chunks[chunk_id]['to']
                            actual_chunk_size = chunk_end - chunk_start + 1
                            
                            f.seek(chunk_start)
                            chunk_data = f.read(actual_chunk_size)
                            
                            # Add to overall hash
                            overall_md5.update(chunk_data)
                            overall_sha1.update(chunk_data)
                            overall_sha256.update(chunk_data)
                
                overall_md5_hex = overall_md5.hexdigest()
                overall_sha1_hex = overall_sha1.hexdigest()
                overall_sha256_hex = overall_sha256.hexdigest()
            
            completed_chunks = len(current_chunks)
            timestamp = datetime.now().isoformat()
            
            # Load existing chunk states to preserve timestamps
            existing_chunk_states = {}
            if json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        existing_json = json.load(f)
                    existing_chunk_states = existing_json.get('chunk_states', {})
                except Exception as e:
                    self.logger.warning(f"Failed to load existing chunk states: {e}")
            
            # Create JSON data structure with enhanced chunk state tracking
            json_data = {
                "file_name": blob_path.name,
                "available": completed_chunks == total_chunks,
                "total_size": expected_size,
                "total_chunks": total_chunks,
                "completed_chunks": completed_chunks,
                "timestamp": timestamp,
                "overall_hashes": {
                    "md5": overall_md5_hex,
                    "sha1": overall_sha1_hex,
                    "sha256": overall_sha256_hex
                },
                "completed_chunk_ids": sorted(current_chunks.keys()),
                "chunk_states": {},  # Track download/validation state per chunk
                "chunk_hashes": {}
            }
            
            # Add chunk hash data for completed chunks only
            for chunk_id in sorted(current_chunks.keys()):
                chunk = current_chunks[chunk_id]
                json_data["chunk_hashes"][str(chunk_id)] = {
                    "from": chunk["from"],
                    "to": chunk["to"],
                    "md5": chunk["md5"],
                    "sha1": chunk["sha1"],
                    "sha256": chunk["sha256"]
                }
                
                chunk_id_str = str(chunk_id)
                
                # Preserve existing timestamps if chunk already existed and was validated
                # Only update timestamps for newly processed chunks
                if (chunk_id_str in existing_chunk_states and 
                    existing_chunk_states[chunk_id_str].get('status') == 'validated' and
                    (newly_processed_chunks is None or chunk_id not in newly_processed_chunks)):
                    # Keep existing timestamps - chunk was already processed in a previous session
                    json_data["chunk_states"][chunk_id_str] = existing_chunk_states[chunk_id_str].copy()
                else:
                    # New chunk or re-validated chunk - use current timestamp
                    json_data["chunk_states"][chunk_id_str] = {
                        "status": "validated",  # downloaded, validated, or failed
                        "download_time": timestamp,
                        "validation_time": timestamp,
                        "error_count": 0
                    }
            
            # Write JSON file atomically (write to temp, then rename)
            temp_json_path = json_path.with_suffix('.json.tmp')
            with open(temp_json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, sort_keys=True)
            
            # Atomic rename
            temp_json_path.replace(json_path)
            
            if completed_chunks == total_chunks:
                self.logger.debug(f"✅ Complete JSON updated: {completed_chunks}/{total_chunks} chunks")
            else:
                self.logger.debug(f"📋 Partial JSON updated: {completed_chunks}/{total_chunks} chunks")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update JSON metadata: {e}")
            return False

    def _parse_existing_checksum_json(self, json_path: Path) -> dict:
        """Parse existing JSON checksum file to get chunk metadata"""
        chunks = {}
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract chunk data from JSON structure
            chunk_hashes = data.get('chunk_hashes', {})
            
            for chunk_id_str, chunk_data in chunk_hashes.items():
                chunk_id = int(chunk_id_str)
                chunks[chunk_id] = {
                    'from': chunk_data['from'],
                    'to': chunk_data['to'],
                    'md5': chunk_data['md5'],
                    'sha1': chunk_data['sha1'],
                    'sha256': chunk_data['sha256']
                }
            
            self.logger.info(f"📋 Parsed {len(chunks)} chunks from existing JSON")
            return chunks
        
        except Exception as e:
            self.logger.warning(f"Failed to parse existing JSON {json_path}: {e}")
            return {}

    def _parse_existing_checksum_xml(self, xml_path: Path) -> dict:
        """Parse existing XML checksum file to get chunk metadata (supports both formats)"""
        import xml.etree.ElementTree as ET
    
        chunks = {}
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
        
            for chunk_elem in root.findall('chunk'):
                chunk_id = int(chunk_elem.get('id'))
                
                if chunk_id not in chunks:
                    chunks[chunk_id] = {
                        'from': int(chunk_elem.get('from')),
                        'to': int(chunk_elem.get('to')),
                    }
                
                # Check if this is the new attribute format (has md5/sha1/sha256 attributes)
                if chunk_elem.get('md5'):
                    # New compact format: all hashes as attributes
                    chunks[chunk_id]['md5'] = chunk_elem.get('md5')
                    if chunk_elem.get('sha1'):
                        chunks[chunk_id]['sha1'] = chunk_elem.get('sha1')
                    if chunk_elem.get('sha256'):
                        chunks[chunk_id]['sha256'] = chunk_elem.get('sha256')
                else:
                    # Old format: hash value in text content, method in attribute
                    method = chunk_elem.get('method', 'md5')  # Default to MD5 for compatibility
                    chunks[chunk_id][method] = chunk_elem.text
            
            self.logger.info(f"📋 Parsed {len(chunks)} chunks from existing XML")
            return chunks
        
        except Exception as e:
            self.logger.warning(f"Failed to parse existing XML {xml_path}: {e}")
            return {}

    # def _download_v1_blob_deduplicated(self, game_id: str, platform: str, build_id: str, repository_id: str, blob_url: str, v1_depot_manifests_data: Dict) -> Dict:
    #     """Download a v1 blob file once for all depot manifests that reference it using chunk-based resume"""
    #     result = {'success': False, 'errors': []}
        
    #     try:
    #         # Use the SAME blob storage structure as the working method
    #         # Store blobs at: self.blobs_dir / build_id / "main.bin"
    #         blob_path = self.blobs_dir / build_id / "main.bin"
            
    #         # Check if we already have this blob on disk (file system truth)
    #         if blob_path.exists():
    #             self.logger.info(f"V1 blob {blob_url} already exists on disk at {blob_path}")
    #             result['success'] = True
    #             result['already_exists'] = True
    #             return result
            
    #         # Get secure links for download (same as _download_v1_blob_with_resume)
    #         from gogdl.dl import dl_utils
    #         secure_links = dl_utils.get_secure_link(
    #             self.api_handler, f"/{platform}/{repository_id}/", game_id, generation=1
    #         )
            
    #         if isinstance(secure_links, str):
    #             blob_cdn_url = f"{secure_links}/main.bin"
    #         else:
    #             endpoint = secure_links[0].copy()
    #             endpoint["parameters"]["path"] += "/main.bin"
    #             blob_cdn_url = dl_utils.merge_url_with_params(
    #                 endpoint["url_format"], endpoint["parameters"]
    #             )
            
    #         # Get expected size from server via HEAD request first
    #         head_response = self.api_handler.session.head(blob_cdn_url, timeout=30)
    #         expected_size = int(head_response.headers.get('Content-Length', 0))
    #         self.logger.info(f"Expected blob size from server: {expected_size:,} bytes ({expected_size / (1024**3):.2f} GB)")
            
    #         # Use the SAME chunk-based resume download as the working method
    #         if not self._download_v1_blob_with_resume(game_id, platform, repository_id, build_id, blob_path, expected_size):
    #             result['errors'].append(f"Failed to download blob with resume for {build_id}")
    #             return result

    #         # Generate checksums after successful download (same as working method)
    #         self.logger.info("📋 Generating checksums for downloaded blob...")
    #         if self._generate_blob_checksum_xml(blob_path, expected_size):
    #             self.logger.info("✅ Checksum generation complete")
    #         else:
    #             self.logger.warning("⚠️  Checksum generation failed (download still successful)")

    #         actual_size = blob_path.stat().st_size
            
    #         # Create consistent key for blob tracking (same as working method)
    #         blob_key = f"{game_id}_{build_id}_main.bin"
            
    #         # Create archived blob record (same structure as working method)
    #         archived_blob = ArchivedBlob(
    #             depot_manifest=blob_key,  # Use consistent key instead of blob URL
    #             secure_url=blob_cdn_url,  # Keep full URL including /main.bin
    #             total_size=actual_size,
    #             archive_path=str(blob_path),
    #             first_seen=time.time(),
    #             last_verified=time.time(),
    #             files_contained=[],  # Remove redundant data - use manifests instead
    #             depot_info={
    #                 'game_id': game_id,
    #                 'platform': platform,
    #                 'build_id': build_id,
    #                 'estimated_size': expected_size,
    #                 'actual_size': actual_size,
    #                 'referenced_by_manifests': list(v1_depot_manifests_data.keys())  # List to support multiple manifests
    #             }
    #         )
            
    #         # Store in database using consistent key (same as working method)
    #         self.archived_blobs[blob_key] = archived_blob
            
    #         self.logger.info(f"Archived v1 blob: {blob_key} at {blob_path} ({actual_size} bytes)")
            
    #         result['success'] = True
    #         result['blobs_archived'] = 1
            
    #     except Exception as e:
    #         result['errors'].append(f"Failed to download v1 blob {blob_url}: {e}")
    #         self.logger.error(f"Failed to download v1 blob {blob_url}: {e}")
            
    #     return result

    def _download_v2_chunks(self, game_id: str, chunk_md5s: set, max_workers: int = 4) -> Dict:
        """Download V2 chunks for a game using multi-threaded approach with base URL"""
        # from gogdl.dl import dl_utils
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        result = {'chunks_archived': 0, 'errors': []}
        
        print(f"\nVALIDATING {len(chunk_md5s)} chunks against file system...")
        
        # NEW: File system truth with hash validation
        chunks_to_download = []
        validated_count = 0
        chunk_num = 0
        
        for chunk_md5 in chunk_md5s:
            chunk_num += 1
            print(f"   🔍 [{chunk_num}/{len(chunk_md5s)}] Validating chunk: {chunk_md5}")
            
            if self._validate_chunk_exists_with_hash(chunk_md5):
                validated_count += 1
                print(f"      ✅ Chunk exists and is valid")
            else:
                chunks_to_download.append(chunk_md5)
                print(f"      ❌ Chunk missing or corrupted - will download")
        
        print(f"   ✅ Chunks validated: {validated_count}")
        print(f"   📥 Chunks to download: {len(chunks_to_download)}")
        
        if not chunks_to_download:
            print(f"   🎯 All {len(chunk_md5s)} chunks verified on file system!")
            return result
            
        print(f"\n🚀 V2 CHUNK DOWNLOAD STARTING")
        print(f"   📊 Total chunks referenced: {len(chunk_md5s)}")
        print(f"   📥 New chunks to download: {len(chunks_to_download)}")
        print(f"   🎯 Game ID: {game_id}")
        print(f"   🗂️  Chunks directory: {self.chunks_dir}")
        print(f"   🔧 Max workers: {max_workers}")
        
        try:
            # Get secure links for the game
            secure_links = dl_utils.get_secure_link(
                self.api_handler, "/", game_id, generation=2
            )
            
            if not secure_links:
                result['errors'].append(f"Failed to get secure links for game {game_id}")
                return result
            
            # Build base URL from secure links (avoiding path concatenation)
            base_endpoint = secure_links[0]  # Use first endpoint
            
            # Extract just the domain and token part, then build proper path
            base_url_with_params = dl_utils.merge_url_with_params(
                base_endpoint["url_format"], base_endpoint["parameters"]
            )
            
            # The secure link already contains the path to content-system, 
            # so we need to extract just the root domain + token and rebuild
            # Example: https://gog-cdn-fastly.gog.com/token=.../content-system/v2/store/1207658930/
            if "/content-system/" in base_url_with_params:
                # Split at content-system and rebuild with proper path
                url_parts = base_url_with_params.split("/content-system/")
                base_url = f"{url_parts[0]}/content-system/v2/store/{game_id}/"
            else:
                # Fallback if format is different
                base_url = base_url_with_params.rstrip('/') + f"/content-system/v2/store/{game_id}/"
            
            print(f"   🌐 Base URL: {base_url}")
            
            # Download chunks with threading using base URL
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._download_single_v2_chunk_with_base_url, chunk_md5, base_url): chunk_md5
                    for chunk_md5 in chunks_to_download
                }
                
                completed = 0
                for future in as_completed(futures):
                    chunk_md5 = futures[future]
                    completed += 1
                    try:
                        archived_chunk = future.result()
                        if archived_chunk:
                            result['chunks_archived'] += 1
                            # File system truth - no database storage needed
                            print(f"   ✅ [{completed}/{len(chunks_to_download)}] Downloaded: {chunk_md5}")
                        else:
                            result['errors'].append(f"Failed to download chunk {chunk_md5}")
                            print(f"   ❌ [{completed}/{len(chunks_to_download)}] Failed: {chunk_md5}")
                    except Exception as e:
                        result['errors'].append(f"Exception downloading chunk {chunk_md5}: {e}")
                        print(f"   💥 [{completed}/{len(chunks_to_download)}] Error: {chunk_md5} - {e}")
            
            print(f"\n✅ V2 CHUNK DOWNLOAD COMPLETE")
            print(f"   📊 Chunks downloaded: {result['chunks_archived']}/{len(chunks_to_download)}")
            if result['errors']:
                print(f"   ❌ Errors: {len(result['errors'])}")
                for error in result['errors'][:3]:  # Show first 3 errors
                    print(f"      • {error}")
            
            # Save database to persist downloaded chunks
            if result['chunks_archived'] > 0:
                print(f"   💾 Saving database with {result['chunks_archived']} new chunks...")
                self.save_database()
            
        except Exception as e:
            result['errors'].append(f"Failed to download chunks for {game_id}: {e}")
            self.logger.error(f"Failed to download chunks for {game_id}: {e}")
        
        return result
    
    def _validate_chunk_exists_with_hash(self, chunk_md5: str) -> bool:
        """Validate chunk exists with correct hash - FILE SYSTEM TRUTH"""
        chunk_path = self.chunks_dir / chunk_md5[:2] / chunk_md5[2:4] / chunk_md5
        
        print(f"         🔍 Checking file: {chunk_path}")
        
        # Check file exists
        if not chunk_path.exists():
            print(f"         ❌ File does not exist")
            return False
        
        try:
            # Critical: Validate hash matches filename for integrity
            print(f"         🔐 Validating MD5 hash...")
            with open(chunk_path, 'rb') as f:
                actual_hash = hashlib.md5(f.read()).hexdigest()
            
            expected_hash = chunk_md5.lower()
            matches = actual_hash.lower() == expected_hash
            
            if matches:
                print(f"         ✅ Hash matches: {actual_hash}")
            else:
                print(f"         ⚠️  Hash mismatch! Expected: {expected_hash}, Got: {actual_hash}")
            
            return matches
            
        except Exception as e:
            print(f"         💥 Exception during hash validation: {e}")
            return False

    def validate_archive_comprehensive(self, game_id: str = None, build_id: str = None, platforms: List[str] = None) -> Dict:
        """Comprehensive archive validation for both V1 and V2 builds
        
        Args:
            game_id: Optional game ID to validate specific game
            build_id: Optional build ID to validate specific build  
            platforms: Optional platforms to validate
            
        Returns:
            Dictionary with validation results
        """
        if not platforms:
            platforms = ['windows']
            
        results = {
            'validation_summary': {
                'total_builds_found': 0,
                'v1_builds_validated': 0,
                'v2_builds_validated': 0,
                'validation_passed': 0,
                'validation_failed': 0,
                'chunks_validated': 0,
                'chunks_failed': 0,
                'blobs_validated': 0, 
                'blobs_failed': 0
            },
            'build_results': [],
            'errors': []
        }
        
        print(f"\n🔍 COMPREHENSIVE ARCHIVE VALIDATION")
        print(f"=" * 60)
        
        try:
            # Find builds to validate
            builds_to_validate = []
            
            if game_id and build_id:
                # Validate specific build
                for platform in platforms:
                    build_key = f"{game_id}_{build_id}_{platform}"
                    if build_key in self.archived_builds:
                        builds_to_validate.append(self.archived_builds[build_key])
                    else:
                        print(f"⚠️  Build not found in archive: {build_key}")
            elif game_id:
                # Validate all builds for a game
                for build in self.archived_builds.values():
                    if build.game_id == game_id and build.platform in platforms:
                        builds_to_validate.append(build)
            else:
                # Validate all builds in archive
                for build in self.archived_builds.values():
                    if build.platform in platforms:
                        builds_to_validate.append(build)
            
            results['validation_summary']['total_builds_found'] = len(builds_to_validate)
            print(f"📊 Found {len(builds_to_validate)} builds to validate")
            
            # Validate each build
            for i, build in enumerate(builds_to_validate, 1):
                print(f"\n📋 [{i}/{len(builds_to_validate)}] Validating: {build.game_id}_{build.build_id}_{build.platform}")
                print(f"   Version: v{build.version}")
                print(f"   Repository ID: {build.repository_id}")
                
                if build.version == 1:
                    build_result = self._validate_v1_build(build)
                    if build_result['success']:
                        results['validation_summary']['v1_builds_validated'] += 1
                    else:
                        results['validation_summary']['validation_failed'] += 1
                elif build.version == 2:
                    build_result = self._validate_v2_build(build)
                    if build_result['success']:
                        results['validation_summary']['v2_builds_validated'] += 1
                    else:
                        results['validation_summary']['validation_failed'] += 1
                else:
                    build_result = {
                        'success': False,
                        'build_key': f"{build.game_id}_{build.build_id}_{build.platform}",
                        'errors': [f"Unknown build version: {build.version}"]
                    }
                    results['validation_summary']['validation_failed'] += 1
                
                # Aggregate validation stats
                if build_result['success']:
                    results['validation_summary']['validation_passed'] += 1
                
                # Aggregate chunk/blob stats
                results['validation_summary']['chunks_validated'] += build_result.get('chunks_validated', 0)
                results['validation_summary']['chunks_failed'] += build_result.get('chunks_failed', 0)
                results['validation_summary']['blobs_validated'] += build_result.get('blobs_validated', 0)
                results['validation_summary']['blobs_failed'] += build_result.get('blobs_failed', 0)
                
                results['build_results'].append(build_result)
                results['errors'].extend(build_result.get('errors', []))
            
            # Summary
            print(f"\n🏁 VALIDATION COMPLETE")
            print(f"=" * 40)
            print(f"✅ Builds passed: {results['validation_summary']['validation_passed']}")
            print(f"❌ Builds failed: {results['validation_summary']['validation_failed']}")
            print(f"📦 V1 builds validated: {results['validation_summary']['v1_builds_validated']}")
            print(f"📦 V2 builds validated: {results['validation_summary']['v2_builds_validated']}")
            print(f"🧩 Chunks validated: {results['validation_summary']['chunks_validated']}")
            print(f"🧩 Chunks failed: {results['validation_summary']['chunks_failed']}")  
            print(f"📄 Blobs validated: {results['validation_summary']['blobs_validated']}")
            print(f"📄 Blobs failed: {results['validation_summary']['blobs_failed']}")
            
        except Exception as e:
            error_msg = f"Validation process failed: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results

    def _validate_v2_build(self, build: ArchivedBuild) -> Dict:
        """Validate V2 build by checking all depot manifests and their chunks
        
        For V2: Chunks are validated by comparing their MD5 hash against filename
        """
        result = {
            'success': True,
            'build_key': f"{build.game_id}_{build.build_id}_{build.platform}",
            'version': 2,
            'depot_manifests_found': 0,
            'chunks_validated': 0,
            'chunks_failed': 0,
            'validation_details': [],
            'errors': []
        }
        
        try:
            print(f"   🔍 V2 Build Validation")
            
            # Load build manifest to get depot manifest IDs
            build_manifest_path = self.archive_root / build.archive_path
            if not build_manifest_path.exists():
                result['success'] = False
                result['errors'].append(f"Build manifest not found: {build.archive_path}")
                return result
            
            # Read and parse build manifest
            try:
                with open(build_manifest_path, 'rb') as f:
                    raw_data = f.read()
                
                # Decompress V2 manifest
                if raw_data.startswith(b'\x1f\x8b'):  # gzip
                    manifest_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
                elif raw_data.startswith(b'\x78'):  # zlib
                    manifest_data = json.loads(zlib.decompress(raw_data).decode('utf-8'))
                else:
                    manifest_data = json.loads(raw_data.decode('utf-8'))
            except Exception as e:
                result['success'] = False
                result['errors'].append(f"Failed to parse build manifest: {e}")
                return result
            
            # Extract depot manifest IDs
            depot_manifests = []
            for depot in manifest_data.get('depots', []):
                if 'manifest' in depot:
                    depot_manifests.append(depot['manifest'])
            
            result['depot_manifests_found'] = len(depot_manifests)
            print(f"      📋 Found {len(depot_manifests)} depot manifests to validate")
            
            # Validate each depot manifest and its chunks
            for depot_manifest_id in depot_manifests:
                print(f"      📄 Validating depot: {depot_manifest_id}")
                depot_result = self._validate_v2_depot_manifest(build.game_id, depot_manifest_id)
                
                result['chunks_validated'] += depot_result.get('chunks_validated', 0)
                result['chunks_failed'] += depot_result.get('chunks_failed', 0)
                result['validation_details'].append(depot_result)
                
                if not depot_result.get('success', False):
                    result['success'] = False
                    result['errors'].extend(depot_result.get('errors', []))
            
            if result['success']:
                print(f"      ✅ V2 build validation PASSED ({result['chunks_validated']} chunks)")
            else:
                print(f"      ❌ V2 build validation FAILED ({result['chunks_failed']} chunk failures)")
                
        except Exception as e:
            result['success'] = False
            error_msg = f"V2 build validation failed: {e}"
            result['errors'].append(error_msg)
            print(f"      💥 {error_msg}")
            
        return result

    def _validate_v2_depot_manifest(self, game_id: str, manifest_id: str) -> Dict:
        """Validate V2 depot manifest by checking all its chunks"""
        result = {
            'success': True,
            'depot_manifest_id': manifest_id,
            'chunks_validated': 0,
            'chunks_failed': 0,
            'chunk_results': [],
            'errors': []
        }
        
        try:
            # Find depot manifest file
            galaxy_path = manifest_id if "/" in manifest_id else f"{manifest_id[0:2]}/{manifest_id[2:4]}/{manifest_id}"
            depot_paths = [
                self.archive_root / "manifests" / "v2" / "depots" / galaxy_path,
                self.archive_root / "manifests" / "v2" / "meta" / galaxy_path
            ]
            
            depot_manifest_path = None
            for path in depot_paths:
                if path.exists():
                    depot_manifest_path = path
                    break
            
            if not depot_manifest_path:
                result['success'] = False
                result['errors'].append(f"Depot manifest not found: {manifest_id}")
                return result
            
            # Load and parse depot manifest
            with open(depot_manifest_path, 'rb') as f:
                raw_data = f.read()
            
            if raw_data.startswith(b'\x1f\x8b'):  # gzip
                depot_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
            elif raw_data.startswith(b'\x78'):  # zlib
                depot_data = json.loads(zlib.decompress(raw_data, 15).decode('utf-8'))
            else:
                depot_data = json.loads(raw_data.decode('utf-8'))
            
            # Extract chunks from depot manifest
            chunks_to_validate = set()
            depot_info = depot_data.get('depot', {})
            for file_record in depot_info.get('items', []):
                if file_record.get('type') == 'DepotFile':
                    for chunk in file_record.get('chunks', []):
                        chunk_id = chunk.get('compressedMd5')
                        if chunk_id:
                            chunks_to_validate.add(chunk_id)
            
            print(f"         🧩 Found {len(chunks_to_validate)} unique chunks to validate")
            
            # Validate each chunk
            for chunk_id in chunks_to_validate:
                chunk_valid = self._validate_chunk_exists_with_hash(chunk_id)
                
                chunk_result = {
                    'chunk_id': chunk_id,
                    'valid': chunk_valid
                }
                result['chunk_results'].append(chunk_result)
                
                if chunk_valid:
                    result['chunks_validated'] += 1
                else:
                    result['chunks_failed'] += 1
                    result['success'] = False
            
        except Exception as e:
            result['success'] = False
            error_msg = f"Failed to validate depot manifest {manifest_id}: {e}"
            result['errors'].append(error_msg)
            
        return result

    def _validate_v1_build(self, build: ArchivedBuild) -> Dict:
        """Validate V1 build by reading repository manifest and validating blob contents
        
        For V1: Read repository manifest to get file list with offsets, lengths, and MD5 checksums
        """
        result = {
            'success': True,
            'build_key': f"{build.game_id}_{build.build_id}_{build.platform}",
            'version': 1,
            'blobs_validated': 0,
            'blobs_failed': 0,
            'files_validated': 0,
            'files_failed': 0,
            'validation_details': [],
            'errors': []
        }
        
        try:
            print(f"   🔍 V1 Build Validation")
            
            # Load repository manifest 
            build_manifest_path = self.archive_root / build.archive_path
            if not build_manifest_path.exists():
                result['success'] = False
                result['errors'].append(f"Build manifest not found: {build.archive_path}")
                return result
            
            # Read and parse V1 repository manifest (plain JSON)
            try:
                with open(build_manifest_path, 'rb') as f:
                    raw_data = f.read()
                
                # V1 manifests might be compressed or plain JSON
                try:
                    if raw_data.startswith(b'\x1f\x8b'):  # gzip
                        manifest_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
                    elif raw_data.startswith(b'\x78'):  # zlib
                        manifest_data = json.loads(zlib.decompress(raw_data).decode('utf-8'))
                    else:
                        manifest_data = json.loads(raw_data.decode('utf-8'))
                except:
                    # Try as plain JSON
                    manifest_data = json.loads(raw_data.decode('utf-8'))
            except Exception as e:
                result['success'] = False
                result['errors'].append(f"Failed to parse repository manifest: {e}")
                return result
            
            # Extract depot information from V1 manifest
            product_data = manifest_data.get('product', {})
            depots = product_data.get('depots', [])
            
            print(f"      📋 Found {len(depots)} depots to validate")
            
            # NEW: Collect files from ALL manifest depots first, then sort by offset
            all_files_to_validate = []
            manifest_depots_processed = 0
            
            for depot in depots:
                # Skip non-manifest depots (redist depots)
                depot_manifest_id = depot.get('manifest')
                if not depot_manifest_id:
                    print(f"         ⏭️  Skipping non-manifest depot (redist/executable): {depot.get('redist', depot.get('executable', 'unknown'))}")
                    continue
                
                manifest_depots_processed += 1
                print(f"         📄 Processing manifest depot: {depot_manifest_id}")
                
                # Load depot manifest to extract files
                depot_manifest_path = self.archive_root / "manifests" / "v1" / "manifests" / build.game_id / build.platform / str(build.repository_id) / depot_manifest_id
                
                if not depot_manifest_path.exists():
                    error_msg = f"Depot manifest not found: {depot_manifest_path}"
                    result['errors'].append(error_msg)
                    result['success'] = False
                    continue
                
                try:
                    # Load depot manifest
                    with open(depot_manifest_path, 'rb') as f:
                        raw_data = f.read()
                    
                    # Parse JSON
                    try:
                        depot_data = json.loads(raw_data.decode('utf-8'))
                    except:
                        if raw_data.startswith(b'\x1f\x8b'):
                            depot_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
                        else:
                            raise
                    
                    # Extract files from this depot
                    depot_files = depot_data.get('depot', {}).get('files', [])
                    depot_file_count = 0
                    
                    for file_item in depot_files:
                        blob_url = file_item.get('url', '')
                        if blob_url:
                            blob_id = blob_url.split('/')[0] if '/' in blob_url else blob_url.replace('.bin', '')
                            
                            file_info = {
                                'path': file_item.get('path', ''),
                                'size': file_item.get('size', 0),
                                'md5': file_item.get('hash', ''),
                                'blob_id': blob_id,
                                'blob_offset': file_item.get('offset', 0),
                                'blob_length': file_item.get('size', 0),
                                'depot_manifest': depot_manifest_id  # Track which depot this came from
                            }
                            all_files_to_validate.append(file_info)
                            depot_file_count += 1
                    
                    print(f"            📁 Extracted {depot_file_count} files from depot {depot_manifest_id}")
                    
                except Exception as e:
                    error_msg = f"Failed to process depot manifest {depot_manifest_id}: {e}"
                    result['errors'].append(error_msg)
                    result['success'] = False
            
            print(f"      📊 Manifest depots processed: {manifest_depots_processed}")
            print(f"      📁 Total files collected: {len(all_files_to_validate)}")
            
            if not all_files_to_validate:
                print(f"      ⚠️  No files found to validate")
                return result
            
            # Sort all files by offset for efficient blob reading
            all_files_to_validate.sort(key=lambda f: f['blob_offset'])
            print(f"      🔄 Files sorted by offset (range: {all_files_to_validate[0]['blob_offset']:,} - {all_files_to_validate[-1]['blob_offset']:,})")
            
            # Group files by blob (should all be the same blob for V1)
            blobs_to_validate = {}
            for file_info in all_files_to_validate:
                blob_id = file_info['blob_id']
                if blob_id not in blobs_to_validate:
                    blobs_to_validate[blob_id] = []
                blobs_to_validate[blob_id].append(file_info)
            
            # Validate each blob with its sorted file list
            for blob_id, blob_files in blobs_to_validate.items():
                actual_blob_id = build.build_id if build.build_id else blob_id
                print(f"      🗄️  Validating blob: {actual_blob_id} ({len(blob_files)} files, sorted by offset)")
                blob_result = self._validate_v1_blob_files_sorted(actual_blob_id, blob_files)
                
                result['files_validated'] += blob_result.get('files_validated', 0)
                result['files_failed'] += blob_result.get('files_failed', 0)
                result['validation_details'].append(blob_result)
                
                if blob_result.get('blob_valid', False):
                    result['blobs_validated'] += 1
                else:
                    result['blobs_failed'] += 1
                    result['success'] = False
                
                if not blob_result.get('success', False):
                    result['errors'].extend(blob_result.get('errors', []))
            
            if result['success']:
                print(f"      ✅ V1 build validation PASSED ({result['files_validated']} files, {result['blobs_validated']} blobs)")
            else:
                print(f"      ❌ V1 build validation FAILED ({result['files_failed']} file failures, {result['blobs_failed']} blob failures)")
                
        except Exception as e:
            result['success'] = False
            error_msg = f"V1 build validation failed: {e}"
            result['errors'].append(error_msg)
            print(f"      💥 {error_msg}")
            
        return result

    def _validate_v1_depot(self, game_id: str, platform: str, repository_id: str, depot_info: Dict, build_id: str = None) -> Dict:
        """Validate V1 depot by checking blob contents against file manifest"""
        result = {
            'success': True,
            'depot_id': depot_info.get('id', 'unknown'),
            'blobs_validated': 0,
            'blobs_failed': 0,
            'files_validated': 0,
            'files_failed': 0,
            'file_results': [],
            'errors': []
        }
        
        try:
            # Get depot manifest ID
            depot_manifest_id = depot_info.get('manifest')
            if not depot_manifest_id:
                # Skip non-manifest depots (like redist depots)
                print(f"         ⏭️  Skipping non-manifest depot (redist/executable): {depot_info.get('redist', depot_info.get('executable', 'unknown'))}")
                result['success'] = True  # Don't fail for redist depots
                return result
            
            print(f"         📄 Validating V1 depot: {depot_manifest_id}")
            
            # Find depot manifest file - convert repository_id to string for path construction
            depot_manifest_path = self.archive_root / "manifests" / "v1" / "manifests" / game_id / platform / str(repository_id) / depot_manifest_id
            
            if not depot_manifest_path.exists():
                result['success'] = False
                result['errors'].append(f"Depot manifest not found: {depot_manifest_path}")
                return result
            
            # Load depot manifest
            with open(depot_manifest_path, 'rb') as f:
                raw_data = f.read()
            
            # V1 depot manifests are typically plain JSON
            try:
                depot_data = json.loads(raw_data.decode('utf-8'))
            except:
                # Try decompression if needed
                if raw_data.startswith(b'\x1f\x8b'):
                    depot_data = json.loads(gzip.decompress(raw_data).decode('utf-8'))
                else:
                    raise
            
            # Extract file list with blob references
            files_to_validate = []
            depot_files = depot_data.get('depot', {}).get('files', [])  # V1 uses 'files' not 'items'
            
            for file_item in depot_files:
                # V1 files have different structure - they directly contain blob info
                blob_url = file_item.get('url', '')
                if blob_url:  # V1 files reference blobs via 'url' field
                    # Extract blob ID from URL (e.g., "1207658930/main.bin" -> "1207658930")
                    blob_id = blob_url.split('/')[0] if '/' in blob_url else blob_url.replace('.bin', '')
                    
                    file_info = {
                        'path': file_item.get('path', ''),
                        'size': file_item.get('size', 0),
                        'md5': file_item.get('hash', ''),  # V1 uses 'hash' not 'md5'
                        'blob_id': blob_id,
                        'blob_offset': file_item.get('offset', 0),
                        'blob_length': file_item.get('size', 0)  # In V1, length = size
                    }
                    files_to_validate.append(file_info)
            
            print(f"            📁 Found {len(files_to_validate)} files to validate")
            
            # Group files by blob for efficient validation
            blobs_to_validate = {}
            for file_info in files_to_validate:
                blob_id = file_info['blob_id']
                if blob_id not in blobs_to_validate:
                    blobs_to_validate[blob_id] = []
                blobs_to_validate[blob_id].append(file_info)
            
            # Validate each blob and its contained files
            for blob_id, blob_files in blobs_to_validate.items():
                # Use build_id instead of blob_id for the directory name
                actual_blob_id = build_id if build_id else blob_id
                blob_result = self._validate_v1_blob_files(actual_blob_id, blob_files)
                
                result['files_validated'] += blob_result.get('files_validated', 0)
                result['files_failed'] += blob_result.get('files_failed', 0)
                result['file_results'].extend(blob_result.get('file_results', []))
                
                if blob_result.get('blob_valid', False):
                    result['blobs_validated'] += 1
                else:
                    result['blobs_failed'] += 1
                    result['success'] = False
                
                if not blob_result.get('success', False):
                    result['errors'].extend(blob_result.get('errors', []))
            
        except Exception as e:
            result['success'] = False
            error_msg = f"Failed to validate V1 depot {depot_info.get('id', 'unknown')}: {e}"
            result['errors'].append(error_msg)
            
        return result

    def _validate_v1_blob_files_sorted(self, blob_id: str, files: List[Dict]) -> Dict:
        """Validate files within a V1 blob using sorted file list for efficient sequential reading"""
        result = {
            'success': True,
            'blob_id': blob_id,
            'blob_valid': False,
            'files_validated': 0,
            'files_failed': 0,
            'file_results': [],
            'errors': []
        }
        
        try:
            # Find blob file (main.bin)
            blob_path = self.blobs_dir / blob_id / "main.bin"
            
            print(f"         🗄️  Validating blob: {blob_id} ({len(files)} files, sorted by offset)")
            
            if not blob_path.exists():
                result['errors'].append(f"Blob file not found: {blob_path}")
                result['success'] = False
                return result
            
            result['blob_valid'] = True
            
            # Show offset range and depot distribution for sorted files
            if files:
                offset_start = files[0]['blob_offset']
                offset_end = files[-1]['blob_offset'] + files[-1]['blob_length']
                print(f"            📏 Offset range: {offset_start:,} - {offset_end:,} bytes")
                
                # Show depot distribution
                depot_counts = {}
                for file_info in files:
                    depot = file_info.get('depot_manifest', 'unknown')
                    depot_counts[depot] = depot_counts.get(depot, 0) + 1
                
                print(f"            📋 Files by depot: {dict(depot_counts)}")
            
            # Validate files with efficient sequential blob reading
            with open(blob_path, 'rb') as blob_file:
                current_position = 0
                
                for i, file_info in enumerate(files, 1):
                    file_result = {
                        'file_path': file_info['path'],
                        'expected_size': file_info['size'],
                        'expected_md5': file_info['md5'],
                        'blob_offset': file_info['blob_offset'],
                        'blob_length': file_info['blob_length'],
                        'depot_manifest': file_info.get('depot_manifest', 'unknown'),
                        'valid': False,
                        'actual_size': 0,
                        'actual_md5': ''
                    }
                    
                    try:
                        # Check for overlap detection (files should not overlap)
                        if i > 1:
                            prev_file = files[i-2]  # Previous file (0-indexed)
                            prev_end = prev_file['blob_offset'] + prev_file['blob_length']
                            current_start = file_info['blob_offset']
                            
                            if current_start < prev_end:
                                print(f"            ⚠️  File overlap detected: {prev_file['path']} ends at {prev_end}, {file_info['path']} starts at {current_start}")
                        
                        # Seek to file offset (efficient since files are sorted)
                        if blob_file.tell() != file_info['blob_offset']:
                            blob_file.seek(file_info['blob_offset'])
                            current_position = file_info['blob_offset']
                        
                        # Read file data
                        file_data = blob_file.read(file_info['blob_length'])
                        current_position += len(file_data)
                        file_result['actual_size'] = len(file_data)
                        
                        # Validate size
                        if len(file_data) != file_info['size']:
                            file_result['valid'] = False
                            result['files_failed'] += 1
                            print(f"            ❌ Size mismatch: {file_info['path']} (expected {file_info['size']}, got {len(file_data)})")
                        else:
                            # Validate MD5 checksum
                            actual_md5 = hashlib.md5(file_data).hexdigest()
                            file_result['actual_md5'] = actual_md5
                            
                            if actual_md5.lower() == file_info['md5'].lower():
                                file_result['valid'] = True
                                result['files_validated'] += 1
                                
                                # Show progress every 100 files or for first/last few files
                                if i <= 3 or i >= len(files) - 2 or i % 100 == 0:
                                    print(f"            ✅ [{i}/{len(files)}] {file_info['path']} (offset: {file_info['blob_offset']:,}, size: {file_info['size']:,})")
                            else:
                                file_result['valid'] = False
                                result['files_failed'] += 1
                                result['success'] = False
                                print(f"            ❌ MD5 mismatch: {file_info['path']} (expected {file_info['md5']}, got {actual_md5})")
                    
                    except Exception as e:
                        file_result['valid'] = False
                        result['files_failed'] += 1
                        result['success'] = False
                        result['errors'].append(f"Failed to validate file {file_info['path']}: {e}")
                        print(f"            💥 Error validating {file_info['path']}: {e}")
                    
                    result['file_results'].append(file_result)
            
            print(f"            🏁 Validation complete: ✅ {result['files_validated']} valid, ❌ {result['files_failed']} failed")
            
        except Exception as e:
            result['success'] = False
            error_msg = f"Failed to validate blob {blob_id}: {e}"
            result['errors'].append(error_msg)
            
        return result

    def _validate_v1_blob_files(self, blob_id: str, files: List[Dict]) -> Dict:
        """Validate files within a V1 blob by reading at specified offsets and checking MD5"""
        result = {
            'success': True,
            'blob_id': blob_id,
            'blob_valid': False,
            'files_validated': 0,
            'files_failed': 0,
            'file_results': [],
            'errors': []
        }
        
        try:
            # Find blob file (main.bin)
            blob_path = self.blobs_dir / blob_id / "main.bin"
            
            print(f"               🗄️  Validating blob: {blob_id} ({len(files)} files)")
            
            if not blob_path.exists():
                result['errors'].append(f"Blob file not found: {blob_path}")
                result['success'] = False
                return result
            
            result['blob_valid'] = True
            
            # Validate each file in the blob
            with open(blob_path, 'rb') as blob_file:
                for file_info in files:
                    file_result = {
                        'file_path': file_info['path'],
                        'expected_size': file_info['size'],
                        'expected_md5': file_info['md5'],
                        'blob_offset': file_info['blob_offset'],
                        'blob_length': file_info['blob_length'],
                        'valid': False,
                        'actual_size': 0,
                        'actual_md5': ''
                    }
                    
                    try:
                        # Seek to file offset in blob
                        blob_file.seek(file_info['blob_offset'])
                        
                        # Read file data
                        file_data = blob_file.read(file_info['blob_length'])
                        file_result['actual_size'] = len(file_data)
                        
                        # Validate size
                        if len(file_data) != file_info['size']:
                            file_result['valid'] = False
                            result['files_failed'] += 1
                        else:
                            # Validate MD5 checksum
                            actual_md5 = hashlib.md5(file_data).hexdigest()
                            file_result['actual_md5'] = actual_md5
                            
                            if actual_md5.lower() == file_info['md5'].lower():
                                file_result['valid'] = True
                                result['files_validated'] += 1
                            else:
                                file_result['valid'] = False
                                result['files_failed'] += 1
                                result['success'] = False
                    
                    except Exception as e:
                        file_result['valid'] = False
                        result['files_failed'] += 1
                        result['success'] = False
                        result['errors'].append(f"Failed to validate file {file_info['path']}: {e}")
                    
                    result['file_results'].append(file_result)
            
            print(f"                  ✅ Files valid: {result['files_validated']}, ❌ Failed: {result['files_failed']}")
            
        except Exception as e:
            result['success'] = False
            error_msg = f"Failed to validate blob {blob_id}: {e}"
            result['errors'].append(error_msg)
            
        return result

    def _download_single_v2_chunk_with_base_url(self, chunk_md5: str, base_url: str):
        """Download a single V2 chunk using base URL + chunk path (prevents URL concatenation)"""
        
        try:
            # Build chunk URL: base_url + galaxy_path format
            galaxy_path = dl_utils.galaxy_path(chunk_md5)  # Returns: "f7/32/f732fe8750ba3a2f86dea9496f208b69"
            chunk_url = f"{base_url}{galaxy_path}"
            
            # Display URL and save path for verification
            expected_save_path = self.chunks_dir / chunk_md5[:2] / chunk_md5[2:4] / chunk_md5
            print(f"📦 Downloading chunk: {chunk_md5}")
            print(f"   🌐 URL: {chunk_url}")  
            print(f"   🗂️  Galaxy path: {galaxy_path}")
            print(f"   💾 Save to: {expected_save_path}")
            
            # Download chunk
            response = self.api_handler.session.get(chunk_url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Save chunk to archive using compressedMd5 directory structure
            chunk_path = self._save_raw_chunk(chunk_md5, response.content)
            
            # Verify hash matches (compressedMd5 should match what we downloaded)
            actual_hash = hashlib.md5(response.content).hexdigest()
            if actual_hash != chunk_md5:
                self.logger.error(f"Chunk hash mismatch: expected {chunk_md5}, got {actual_hash}")
                return None
            
            print(f"   ✅ Hash verified: {actual_hash}")
            print(f"   💾 Saved to: {chunk_path}")
            
            # Create archived chunk record
            archived_chunk = ArchivedChunk(
                md5=chunk_md5,  # This is the compressedMd5
                sha256=None,  # Not provided in V2 manifests
                compressed_size=len(response.content),
                archive_path=chunk_path,
                cdn_path=galaxy_path,
                first_seen=time.time(),
                last_verified=time.time()
            )
            
            return archived_chunk
            
        except Exception as e:
            self.logger.error(f"Failed to download chunk {chunk_md5}: {e}")
            return None

    def _download_single_v2_chunk(self, chunk_md5: str, secure_links: list, game_id: str):
        """Download a single V2 chunk using the galaxy_path URL structure"""
        
        try:
            # Build chunk URL using galaxy_path format (same as existing V2 downloader)
            endpoint = secure_links[0].copy()
            galaxy_path = dl_utils.galaxy_path(chunk_md5)
            endpoint["parameters"]["path"] += f"/{galaxy_path}"
            url = dl_utils.merge_url_with_params(
                endpoint["url_format"], endpoint["parameters"]
            )
            
            # Display URL and save path for verification
            expected_save_path = self.chunks_dir / chunk_md5[:2] / chunk_md5[2:4] / chunk_md5
            print(f"📦 Downloading V2 chunk: {chunk_md5}")
            print(f"   🌐 URL: {url}")
            print(f"   🗂️  Galaxy path: {galaxy_path}")
            print(f"   💾 Save to: {expected_save_path}")
            
            # Download chunk
            response = self.api_handler.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Save chunk to archive using the same directory structure as chunks_dir
            chunk_path = self._save_raw_chunk(chunk_md5, response.content)
            
            # Verify hash matches
            actual_hash = hashlib.md5(response.content).hexdigest()
            if actual_hash != chunk_md5:
                self.logger.error(f"Chunk hash mismatch: expected {chunk_md5}, got {actual_hash}")
                return None
            
            # Create archived chunk record
            archived_chunk = ArchivedChunk(
                md5=chunk_md5,
                sha256=None,  # Not provided in V2 manifests
                compressed_size=len(response.content),
                archive_path=chunk_path,
                cdn_path=dl_utils.galaxy_path(chunk_md5),
                first_seen=time.time(),
                last_verified=time.time()
            )
            
            return archived_chunk
            
        except Exception as e:
            self.logger.error(f"Failed to download chunk {chunk_md5}: {e}")
            return None
        
    def archive_chunks_for_manifest(self, archived_build: ArchivedBuild, 
                                  max_workers: int = 4, specific_manifest_id: str = None) -> List[ArchivedChunk]:
        """Archive all chunks referenced by a v2 manifest"""
        chunks_to_download = []
        
        # Find chunks we don't already have
        for content_id in archived_build.manifests_referenced:
            if content_id and content_id not in self.archived_chunks:
                chunks_to_download.append(content_id)
                
        if not chunks_to_download:
            self.logger.info(f"All chunks already archived for {archived_build.game_id}")
            return []
            
        self.logger.info(f"Downloading {len(chunks_to_download)} new chunks for {archived_build.game_id}")
        
        # Get secure links for the game
        secure_links = dl_utils.get_secure_link(
            self.api_handler, "/", archived_build.game_id
        )
        
        archived_chunks = []
        
        # Download chunks with threading
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._archive_chunk, chunk_md5, secure_links, archived_build.game_id): chunk_md5
                for chunk_md5 in chunks_to_download
            }
            
            for future in as_completed(futures):
                chunk_md5 = futures[future]
                try:
                    archived_chunk = future.result()
                    if archived_chunk:
                        archived_chunks.append(archived_chunk)
                except Exception as e:
                    self.logger.error(f"Failed to archive chunk {chunk_md5}: {e}")
                    
        return archived_chunks
        
    def archive_blobs_for_manifest(self, archived_build: ArchivedBuild,
                                 max_workers: int = 4, specific_manifest_id: str = None) -> List[ArchivedBlob]:
        """Archive all blobs referenced by a v1 manifest"""
        # Re-load the manifest to get depot details
        manifest_path = Path(archived_build.archive_path)
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
            
        # Parse v1 manifest to get depot info  
        manifest_obj = v1.Manifest(archived_build.platform, manifest_data, None, [], self.api_handler, False)
        manifest_obj.get_files()
        
        # Collect depot blobs to download
        blobs_to_download = {}
        total_expected_bytes = 0
        
        for depot in manifest_obj.depots:
            depot_manifest = depot.manifest
            if depot_manifest not in self.archived_blobs:
                # Get files for this depot by re-parsing the depot manifest
                depot_manifest_data = dl_utils.get_json(
                    self.api_handler, 
                    f"{constants.GOG_CDN}/content-system/v1/manifests/{depot.game_ids[0]}/{archived_build.platform}/{archived_build.build_id}/{depot_manifest}"
                )
                
                files_in_depot = []
                for record in depot_manifest_data["depot"]["files"]:
                    if "directory" not in record:  # Skip directories
                        files_in_depot.append({
                            'path': record["path"].lstrip("/"),
                            'offset': record.get("offset", 0),
                            'size': record["size"],
                            'hash': record.get("hash", "")
                        })
                
                # Calculate expected size from depot info
                expected_size = depot.size if hasattr(depot, 'size') and depot.size else 0
                total_expected_bytes += expected_size
                
                blobs_to_download[depot_manifest] = {
                    'depot': depot,
                    'files': files_in_depot,
                    'game_id': depot.game_ids[0],
                    'expected_size': expected_size
                }
                    
        if not blobs_to_download:
            self.logger.info(f"All blobs already archived for {archived_build.game_id}")
            return []
            
        self.logger.info(f"Downloading {len(blobs_to_download)} new blobs for {archived_build.game_id}")
        
        archived_blobs = []
        
        # Download blobs with threading
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._archive_blob, depot_manifest, blob_info): depot_manifest
                for depot_manifest, blob_info in blobs_to_download.items()
            }
            
            for future in as_completed(futures):
                depot_manifest = futures[future]
                try:
                    archived_blob = future.result()
                    if archived_blob:
                        archived_blobs.append(archived_blob)
                except Exception as e:
                    self.logger.error(f"Failed to archive blob {depot_manifest}: {e}")
                    
        return archived_blobs
        
    def _archive_chunk(self, chunk_md5: str, secure_links: List[dict], 
                      game_id: str) -> Optional[ArchivedChunk]:
        """Archive a single chunk"""
        try:
            # Build chunk URL using existing logic from task_executor
            endpoint = secure_links[0].copy()
            endpoint["parameters"]["path"] += f"/{dl_utils.galaxy_path(chunk_md5)}"
            url = dl_utils.merge_url_with_params(
                endpoint["url_format"], endpoint["parameters"]
            )
            
            # Download chunk
            response = self.api_handler.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Save chunk to archive
            chunk_path = self.chunks_dir / f"{chunk_md5[:2]}" / f"{chunk_md5[2:4]}" / f"{chunk_md5}.chunk"
            chunk_path.parent.mkdir(parents=True, exist_ok=True)
            
            compressed_size = 0
            chunk_hash = hashlib.md5()
            
            with open(chunk_path, 'wb') as f:
                for chunk_data in response.iter_content(chunk_size=8192):
                    f.write(chunk_data)
                    chunk_hash.update(chunk_data)
                    compressed_size += len(chunk_data)
                    
            # Verify hash matches
            if chunk_hash.hexdigest() != chunk_md5:
                self.logger.error(f"Chunk hash mismatch: expected {chunk_md5}, got {chunk_hash.hexdigest()}")
                chunk_path.unlink()
                return None
                
            # Create archived chunk record
            archived_chunk = ArchivedChunk(
                md5=chunk_md5,
                sha256=None,  # Could calculate if needed
                compressed_size=compressed_size,
                archive_path=str(chunk_path),
                cdn_path=dl_utils.galaxy_path(chunk_md5),
                first_seen=time.time(),
                last_verified=time.time()
            )
            
            # Store in database
            self.archived_chunks[chunk_md5] = archived_chunk
            
            self.logger.debug(f"Archived chunk: {chunk_md5} ({compressed_size} bytes)")
            return archived_chunk
            
        except Exception as e:
            self.logger.error(f"Failed to archive chunk {chunk_md5}: {e}")
            return None
            
    def _archive_blob(self, depot_manifest: str, blob_info: Dict) -> Optional[ArchivedBlob]:
        """Archive a single v1 binary blob (main.bin)"""
        try:
            depot = blob_info['depot']
            files_contained = blob_info['files']
            game_id = blob_info['game_id']
            
            # Get secure links for this game
            secure_links = dl_utils.get_secure_link(self.api_handler, "/", game_id)
            
            # Build the main.bin URL just like the existing v1 downloader
            if isinstance(secure_links, str):
                url = secure_links + "/main.bin"
            else:
                endpoint = secure_links[0].copy()
                endpoint["parameters"]["path"] += "/main.bin"
                url = dl_utils.merge_url_with_params(
                    endpoint["url_format"], endpoint["parameters"]
                )
            
            # Download the entire main.bin blob
            response = self.api_handler.session.get(url, stream=True, timeout=120)
            response.raise_for_status()
            
            # Save blob to archive using depot manifest as filename
            blob_path = self.blobs_dir / f"{depot_manifest[:2]}" / f"{depot_manifest[2:4]}" / f"{depot_manifest}.bin"
            blob_path.parent.mkdir(parents=True, exist_ok=True)
            
            total_size = 0
            
            with open(blob_path, 'wb') as f:
                for chunk_data in response.iter_content(chunk_size=8192):
                    f.write(chunk_data)
                    total_size += len(chunk_data)
                    
            # Create archived blob record
            archived_blob = ArchivedBlob(
                depot_manifest=depot_manifest,
                secure_url=url.replace("/main.bin", ""),  # Store base URL
                total_size=total_size,
                archive_path=str(blob_path),
                first_seen=time.time(),
                last_verified=time.time(),
                files_contained=files_contained,
                depot_info={
                    'game_ids': depot.game_ids,
                    'languages': depot.languages,
                    'size': depot.size
                }
            )
            
            # Store in database
            self.archived_blobs[depot_manifest] = archived_blob
            
            self.logger.debug(f"Archived blob: {depot_manifest} ({total_size} bytes, {len(files_contained)} files)")
            return archived_blob
            
        except Exception as e:
            self.logger.error(f"Failed to archive blob {depot_manifest}: {e}")
            return None
            
            self.logger.debug(f"Archived blob: {depot_manifest} ({total_size} bytes, {len(files_contained)} files)")
            return archived_blob
            
        except Exception as e:
            self.logger.error(f"Failed to archive blob {depot_manifest}: {e}")
            return None
            
    def archive_game_complete(self, game_id: str, platforms: List[str] = None, languages: List[str] = None) -> Dict:
        """Complete archive of a game - manifests and all content (chunks/blobs)"""
        results = {
            'game_id': game_id,
            'manifests_archived': 0,
            'chunks_archived': 0,
            'blobs_archived': 0,
            'errors': []
        }
        
        try:
            # Archive manifests
            manifests = self.archive_game_manifests(game_id, platforms)
            results['manifests_archived'] = len(manifests)
            
            # Archive content for each manifest
            for manifest in manifests:
                content_results = self.archive_content_for_manifest(manifest)
                results['chunks_archived'] += content_results['chunks_archived']
                results['blobs_archived'] += content_results['blobs_archived']
                results['errors'].extend(content_results.get('errors', []))
                
            # Save database
            self.save_database()
            
        except Exception as e:
            error_msg = f"Failed to archive game {game_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
        
    def list_builds(self, game_id: str, platforms: List[str] = None, generation: int = None) -> Dict:
        """List available builds for a game with cross-generation search and V1 precedence
        
        Args:
            game_id: GOG game ID
            platforms: List of platforms to query
            generation: API generation (1=V1 builds, 2=V2 builds, None=both)
        """
        if platforms is None:
            platforms = ['windows']
            
        results = {
            'game_id': game_id,
            'builds': []
        }
        
        try:
            for platform in platforms:
                self.logger.info(f"Discovering builds for game {game_id} on {platform}")
                
                # Determine which generations to query
                generations_to_query = []
                if generation is None:
                    # Query both generations to get complete build list - V1 first for precedence
                    generations_to_query = [1, 2]
                else:
                    generations_to_query = [generation]
                
                # Track builds by build_id to handle V1 precedence over V2
                builds_by_id = {}
                
                for gen in generations_to_query:
                    # Build URL with or without generation parameter  
                    if gen == 1:
                        # Generation 1: omit generation parameter (equivalent to generation=1)
                        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds"
                    else:
                        # Generation 2: explicit generation=2
                        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds?generation=2"
                    
                    self.logger.debug(f"Querying generation {gen}: {url}")
                    builds_data = self.api_handler.session.get(url)
                    
                    if not builds_data.ok:
                        self.logger.warning(f"Failed to get builds for {platform} generation {gen}: {builds_data.status_code}")
                        continue
                        
                    builds = builds_data.json()
                    self.logger.debug(f"Generation {gen} returned {len(builds.get('items', []))} builds")
                    
                    for build in builds.get('items', []):
                        build_id = build['build_id']
                        
                        # Determine version based on URL pattern, not generation query
                        link = build.get('link', '')
                        if '/v1/manifests/' in link:
                            version = 1
                        elif '/v2/meta/' in link:
                            version = 2
                        else:
                            version = None  # Unknown version pattern
                        
                        build_info = {
                            'build_id': build_id,
                            'platform': platform,
                            'branch': build.get('branch', 'main'),
                            'legacy': build.get('legacy', False),
                            'date_published': build.get('date_published'),
                            'link': link,
                            'generation_queried': gen,
                            'version': version,
                            'version_name': build.get('version_name', ''),
                            'tags': build.get('tags', []),
                            'legacy_build_id': build.get('legacy_build_id', None),
                            'public': build.get('public', True)
                        }
                        
                        # V1 precedence: if we already have this build_id and current is V1, replace
                        # If we don't have it, or current is V1, store it
                        if build_id not in builds_by_id:
                            builds_by_id[build_id] = build_info
                            self.logger.debug(f"Added new build {build_id} (v{version}) from generation {gen}")
                        elif version == 1:
                            # V1 takes precedence - replace existing entry
                            builds_by_id[build_id] = build_info
                            self.logger.debug(f"V1 build {build_id} takes precedence over existing entry from generation {gen}")
                        else:
                            # We already have this build and current is not V1, keep existing
                            self.logger.debug(f"Skipping duplicate build {build_id} (v{version}) - already have it")
                
                # Convert to list and sort by date_published (newest first)
                all_builds = list(builds_by_id.values())
                all_builds.sort(key=lambda x: x.get('date_published', ''), reverse=True)
                results['builds'].extend(all_builds)
                
                self.logger.info(f"Found {len(all_builds)} builds for {platform} (after V1 precedence)")
                    
        except Exception as e:
            error_msg = f"Failed to list builds for game {game_id}: {e}"
            self.logger.error(error_msg)
            results['error'] = error_msg
            
        return results
        
    def sync_build_metadata(self, game_id: str, platforms: List[str] = None) -> Dict:
        """Sync build metadata (tags, version_name) from API to database for existing builds"""
        if platforms is None:
            platforms = ['windows', 'osx']
            
        results = {
            'game_id': game_id,
            'updated_builds': 0,
            'new_builds_found': 0,
            'errors': []
        }
        
        try:
            # Get current builds from API using our cross-generation method
            api_builds_result = self.list_builds(game_id, platforms=platforms, generation=None)
            
            if 'error' in api_builds_result:
                results['errors'].append(f"Failed to fetch builds from API: {api_builds_result['error']}")
                return results
                
            api_builds = api_builds_result['builds']
            self.logger.info(f"Found {len(api_builds)} builds from API for metadata sync")
            
            updated_count = 0
            new_count = 0
            
            for api_build in api_builds:
                build_id = api_build['build_id']
                platform = api_build['platform']
                
                # Check if we have this build in our database
                build_key = f"{game_id}_{build_id}_{platform}"
                
                if build_key in self.archived_builds:
                    # Update existing build with missing metadata
                    existing_build = self.archived_builds[build_key]
                    
                    # Check if we need to update tags, version_name, or repository_id
                    needs_update = False
                    
                    if not hasattr(existing_build, 'tags') or not existing_build.tags:
                        if api_build.get('tags'):
                            existing_build.tags = api_build['tags']
                            needs_update = True
                            self.logger.debug(f"Updated tags for build {build_id}: {api_build['tags']}")
                    
                    if not hasattr(existing_build, 'version_name') or not existing_build.version_name:
                        if api_build.get('version_name'):
                            existing_build.version_name = api_build['version_name']
                            needs_update = True
                            self.logger.debug(f"Updated version_name for build {build_id}: {api_build['version_name']}")
                    
                    # Update repository_id if missing
                    if not existing_build.repository_id:
                        cdn_url = api_build.get('link', '')
                        repository_id = None
                        
                        if '/v1/manifests/' in cdn_url:
                            # V1: repository ID is the directory containing repository.json
                            url_parts = cdn_url.rstrip('/').split('/')
                            if url_parts[-1] == 'repository.json' and len(url_parts) >= 2:
                                repository_id = url_parts[-2]
                        elif '/v2/meta/' in cdn_url:
                            # V2: repository ID is the manifest hash
                            repository_id = cdn_url.split('/')[-1]
                        
                        if repository_id:
                            existing_build.repository_id = repository_id
                            needs_update = True
                            self.logger.debug(f"Updated repository_id for build {build_id}: {repository_id}")
                    
                    if needs_update:
                        updated_count += 1
                        self.logger.info(f"Updated metadata for build {build_id} ({platform})")
                        
                else:
                    # This is a new build we haven't archived yet - add it with metadata and extract repository_id
                    cdn_url = api_build.get('link', '')
                    
                    # Extract repository_id from CDN URL
                    repository_id = None
                    if '/v1/manifests/' in cdn_url:
                        # V1: repository ID is the directory containing repository.json
                        # Example: .../manifests/1207658930/windows/37794096/repository.json -> 37794096
                        url_parts = cdn_url.rstrip('/').split('/')
                        if url_parts[-1] == 'repository.json' and len(url_parts) >= 2:
                            repository_id = url_parts[-2]  # Directory before repository.json
                    elif '/v2/meta/' in cdn_url:
                        # V2: repository ID is the manifest hash (last part of URL)
                        # Example: .../v2/meta/e5/18/e518c17d90805e8e3998a35fac8b8505 -> e518c17d90805e8e3998a35fac8b8505
                        repository_id = cdn_url.split('/')[-1]
                    
                    new_build = ArchivedBuild(
                        game_id=game_id,
                        build_id=build_id,
                        build_hash="",  # Will be populated when we actually archive the build
                        platform=platform,
                        version=api_build.get('version', 2),
                        archive_path="",  # Will be populated when we actually archive the build
                        cdn_url=cdn_url,
                        timestamp=0,  # Minimal placeholder
                        dependencies=[],  # Minimal placeholder
                        manifests_referenced=set(),  # Minimal placeholder
                        repository_id=repository_id,
                        version_name=api_build.get('version_name', ''),
                        tags=api_build.get('tags', [])
                    )
                    
                    # Store in database
                    self.archived_builds[build_key] = new_build
                    new_count += 1
                    self.logger.info(f"Added new build {build_id} ({platform}) with metadata and repository_id: {repository_id}")
            
            results['updated_builds'] = updated_count
            results['new_builds_found'] = new_count
            
            if updated_count > 0 or new_count > 0:
                # Save database if we made updates or added new builds
                self.save_database()
                self.logger.info(f"Database updated: {updated_count} builds updated, {new_count} builds added")
                
        except Exception as e:
            error_msg = f"Failed to sync metadata for game {game_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
        
    def list_manifests(self, game_id: str, build_id: str, platforms: List[str] = None) -> Dict:
        """List manifests within a specific build using cross-generation search"""
        if platforms is None:
            platforms = ['windows']
            
        results = {
            'game_id': game_id,
            'build_id': build_id,
            'manifests': []
        }
        
        try:
            for platform in platforms:
                self.logger.info(f"Listing manifests for build {build_id} on {platform}")
                
                # Search both generations to find the specific build
                target_build = None
                for generation in [1, 2]:
                    if generation == 1:
                        # Generation 1: omit generation parameter
                        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds"
                    else:
                        # Generation 2: explicit generation=2
                        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds?generation=2"
                    
                    builds_data = self.api_handler.session.get(url)
                    
                    if not builds_data.ok:
                        continue
                        
                    builds = builds_data.json()
                    
                    # Find the specific build
                    for build in builds['items']:
                        if build['build_id'] == build_id:
                            # Determine version based on URL pattern
                            link = build.get('link', '')
                            if '/v1/manifests/' in link:
                                version = 1
                            elif '/v2/meta/' in link:
                                version = 2
                            else:
                                version = None
                            
                            # V1 precedence: if we found V1, use it immediately
                            # If no V1 found yet, use current build
                            if target_build is None or version == 1:
                                target_build = build
                                target_build['version'] = version
                                
                            # If we found V1, no need to continue searching
                            if version == 1:
                                break
                    
                    # If we found V1 build, stop searching other generations
                    if target_build and target_build.get('version') == 1:
                        break
                        
                if not target_build:
                    results['error'] = f"Build {build_id} not found"
                    continue
                    
                # Download and parse build manifest
                manifest_data, headers = dl_utils.get_zlib_encoded(self.api_handler, target_build['link'])
                if not manifest_data:
                    results['error'] = f"Failed to download build manifest"
                    continue
                    
                # Parse manifests from build
                if manifest_data.get('version') == 2:
                    # v2 manifest - has depots list
                    for depot in manifest_data.get('depots', []):
                        manifest_info = {
                            'manifest_id': depot['manifest'],
                            'size': depot['size'],
                            'compressed_size': depot.get('compressedSize', depot['size']),
                            'languages': depot.get('languages', ['*']),
                            'is_gog_depot': depot.get('isGogDepot', False),
                            'platform': platform,
                            'type': 'depot'
                        }
                        results['manifests'].append(manifest_info)
                        
                    # Add offline depot if present (skip for now - offline depot chunks often fail to download)
                    if 'offlineDepot' in manifest_data:
                        offline_depot = manifest_data['offlineDepot']
                        self.logger.info(f"Dry-run: Skipping offline depot manifest: {offline_depot['manifest']} (offline depots not supported)")
                        # manifest_info = {
                        #     'manifest_id': offline_depot['manifest'],
                        #     'size': offline_depot['size'],
                        #     'compressed_size': offline_depot.get('compressedSize', offline_depot['size']),
                        #     'languages': offline_depot.get('languages', ['*']),
                        #     'is_gog_depot': True,
                        #     'platform': platform,
                        #     'type': 'offline_depot'
                        # }
                        # results['manifests'].append(manifest_info)
                        
                else:
                    # v1 manifest - has depot list
                    for depot in manifest_data.get('depots', []):
                        manifest_info = {
                            'manifest_id': depot['manifest'],
                            'size': depot['size'],
                            'languages': depot.get('languages', ['*']),
                            'platform': platform,
                            'type': 'depot_v1'
                        }
                        results['manifests'].append(manifest_info)
                        
                    # Add offline depot if present
                    if 'offlineDepot' in manifest_data:
                        offline_depot = manifest_data['offlineDepot']
                        manifest_info = {
                            'manifest_id': offline_depot['manifest'],
                            'size': offline_depot['size'],
                            'languages': offline_depot.get('languages', ['*']),
                            'platform': platform,
                            'type': 'offline_depot_v1'
                        }
                        results['manifests'].append(manifest_info)
                        
        except Exception as e:
            error_msg = f"Failed to list manifests for build {build_id}: {e}"
            self.logger.error(error_msg)
            results['error'] = error_msg
            
        return results
        
    def archive_build(self, game_id: str, build_id: str, platforms: List[str] = None, languages: List[str] = None, max_workers: int = 4, repository_version: int = None) -> Dict:
        """Archive a specific build OR repository - will use existing archived builds if available"""
        
        # Repository mode: build_id is actually repository_id when repository_version is specified
        if repository_version is not None:
            repository_id = build_id  # In repository mode, build_id parameter contains the repository_id
            return self._archive_repository(game_id, repository_id, repository_version, platforms, languages, max_workers)
        
        # Build ID mode (original behavior)
        results = {
            'game_id': game_id,
            'build_id': build_id,
            'manifests_archived': 0,
            'chunks_archived': 0,
            'blobs_archived': 0,
            'errors': []
        }
        
        try:
            # First, check if we already have this build archived
            existing_builds = []
            for build_key, archived_build in self.archived_builds.items():
                if archived_build.game_id == game_id and archived_build.build_id == build_id:
                    existing_builds.append(archived_build)
                    self.logger.info(f"Found existing archived build: {build_key}")
            
            # If no existing builds found, try to discover and archive manifests
            if not existing_builds:
                if platforms is None:
                    platforms = ['windows']
                manifests = self.archive_build_manifests(game_id, build_id, platforms)
                results['manifests_archived'] = len(manifests)
                existing_builds = manifests
            else:
                self.logger.info(f"Using {len(existing_builds)} existing archived builds")
            
            # Archive content for each manifest
            for manifest in existing_builds:
                content_results = self.archive_content_for_manifest(manifest)
                results['chunks_archived'] += content_results['chunks_archived']
                results['blobs_archived'] += content_results['blobs_archived']
                results['errors'].extend(content_results.get('errors', []))
                
            # Save database
            self.save_database()
            
        except Exception as e:
            error_msg = f"Failed to archive build {build_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results

    # def _archive_repository(self, game_id: str, repository_id: str, repository_version: int, platforms: List[str] = None, languages: List[str] = None, max_workers: int = 4) -> Dict:
    #     """Archive content using repository ID and API version"""
    #     results = {
    #         'game_id': game_id,
    #         'repository_id': repository_id,
    #         'repository_version': repository_version,
    #         'manifests_archived': 0,
    #         'chunks_archived': 0,
    #         'blobs_archived': 0,
    #         'errors': []
    #     }
        
    #     try:
    #         self.logger.info(f"Repository mode: downloading repository {repository_id} using V{repository_version} API")
            
    #         if platforms is None:
    #             platforms = ['windows']
            
    #         # Archive repository build manifest using the specified API version
    #         manifests = self.archive_repository_build_manifests(game_id, repository_id, repository_version, platforms)
    #         results['manifests_archived'] = len(manifests)
            
    #         # Archive content for each manifest
    #         for manifest in manifests:
    #             content_results = self.archive_content_for_manifest(manifest, max_workers)
    #             results['chunks_archived'] += content_results['chunks_archived']
    #             results['blobs_archived'] += content_results['blobs_archived']
    #             results['errors'].extend(content_results.get('errors', []))
            
    #         # Save database
    #         self.save_database()
            
    #     except Exception as e:
    #         error_msg = f"Failed to archive repository {repository_id}: {e}"
    #         self.logger.error(error_msg)
    #         results['errors'].append(error_msg)
            
    #     return results
        
    def archive_manifest(self, game_id: str, build_id: str, manifest_id: str) -> Dict:
        """Archive a specific manifest"""
        results = {
            'game_id': game_id,
            'build_id': build_id,
            'manifest_id': manifest_id,
            'chunks_archived': 0,
            'blobs_archived': 0,
            'errors': []
        }
        
        try:
            # Find the manifest in our archived manifests or get it
            manifest_key = f"{game_id}_{build_id}_windows"  # Assume windows for now
            
            if manifest_key not in self.archived_builds:
                # Need to archive the build manifest first
                self.archive_build_manifests(game_id, build_id, ['windows'])
                
            if manifest_key in self.archived_builds:
                archived_build = self.archived_builds[manifest_key]
                
                # Archive content for this specific manifest
                content_results = self.archive_content_for_manifest(archived_build, specific_manifest_id=manifest_id)
                results['chunks_archived'] = content_results['chunks_archived']
                results['blobs_archived'] = content_results['blobs_archived']
                results['errors'] = content_results.get('errors', [])
                
                # Save database
                self.save_database()
            else:
                results['errors'].append(f"Could not find or archive build manifest for {build_id}")
                
        except Exception as e:
            error_msg = f"Failed to archive manifest {manifest_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
    
    def _archive_repository(self, game_id: str, repository_id: str, repository_version: int, platforms: List[str] = None, languages: List[str] = None, max_workers: int = 4) -> Dict:
        """Archive content using repository ID and API version"""
        results = {
            'game_id': game_id,
            'repository_id': repository_id,
            'repository_version': repository_version,
            'manifests_archived': 0,
            'chunks_archived': 0,
            'blobs_archived': 0,
            'errors': []
        }
        
        if platforms is None:
            platforms = ['windows']
        
        try:
            print(f"\n🎯 REPOSITORY MODE: {repository_id} (V{repository_version} API)")
            print(f"   🎮 Game ID: {game_id}")
            print(f"   🌐 Platforms: {platforms}")
            
            # Build the repository URL based on API version
            if repository_version == 1:
                # V1 API: /v1/manifests/{game_id}/{platform}/{repository_id}/repository.json
                for platform in platforms:
                    repository_url = f"{constants.GOG_CDN}/content-system/v1/manifests/{game_id}/{platform}/{repository_id}/repository.json"
                    print(f"   🔗 V1 Repository URL: {repository_url}")
                    
                    # Download the repository manifest
                    response = self.api_handler.session.get(repository_url)
                    if not response.ok:
                        error_msg = f"Failed to download V1 repository manifest: {response.status_code} - {repository_url}"
                        results['errors'].append(error_msg)
                        continue
                    
                    # Archive the repository as a build manifest
                    archived_build = self._archive_manifest(
                        game_id=game_id, 
                        build_id=repository_id,  # Use repository_id as build_id for storage
                        platform=platform, 
                        manifest_data=response.json(), 
                        cdn_url=repository_url, 
                        raw_data=response.content,
                        repository_id=repository_id
                    )
                    
                    if archived_build:
                        results['manifests_archived'] += 1
                        
                        # Archive content for this build
                        content_results = self.archive_content_for_manifest(archived_build, max_workers)
                        results['chunks_archived'] += content_results['chunks_archived']
                        results['blobs_archived'] += content_results['blobs_archived']
                        results['errors'].extend(content_results.get('errors', []))
                    
            elif repository_version == 2:
                # V2 API: Use downloadable-manifests-collector for repository manifests
                for platform in platforms: 
                    # V2 repositories use galaxy_path format for the repository ID
                    galaxy_path = repository_id
                    if "/" not in galaxy_path:
                        galaxy_path = f"{repository_id[:2]}/{repository_id[2:4]}/{repository_id}"
                    
                    repository_url = f"{constants.GOG_MANIFESTS_COLLECTOR}/manifests/builds/{galaxy_path}"
                    print(f"   🔗 V2 Repository URL: {repository_url}")
                    
                    # Download the repository manifest
                    response = self.api_handler.session.get(repository_url)  
                    if not response.ok:
                        error_msg = f"Failed to download V2 repository manifest: {response.status_code} - {repository_url}"
                        results['errors'].append(error_msg)
                        continue
                    
                    # V2 manifests might be compressed
                    try:
                        if response.content.startswith(b'\x1f\x8b'):  # gzip
                            import gzip
                            manifest_data = json.loads(gzip.decompress(response.content).decode('utf-8'))
                        elif response.content.startswith(b'\x78'):  # zlib
                            manifest_data = json.loads(zlib.decompress(response.content).decode('utf-8'))
                        else:
                            manifest_data = response.json()
                    except Exception as e:
                        error_msg = f"Failed to parse V2 repository manifest: {e}"
                        results['errors'].append(error_msg)
                        continue
                    
                    # Archive the repository as a build manifest
                    archived_build = self._archive_manifest(
                        game_id=game_id,
                        build_id=repository_id,  # Use repository_id as build_id for storage
                        platform=platform,
                        manifest_data=manifest_data,
                        cdn_url=repository_url,
                        raw_data=response.content,
                        repository_id=repository_id
                    )
                    
                    if archived_build:
                        results['manifests_archived'] += 1
                        
                        # Archive content for this build  
                        content_results = self.archive_content_for_manifest(archived_build, max_workers)
                        results['chunks_archived'] += content_results['chunks_archived']
                        results['blobs_archived'] += content_results['blobs_archived']
                        results['errors'].extend(content_results.get('errors', []))
            else:
                results['errors'].append(f"Unsupported repository version: {repository_version}")
            
            # Save database
            self.save_database()
            
        except Exception as e:
            error_msg = f"Failed to archive repository {repository_id}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
        
    def verify_chunk_integrity(self, chunk_md5: str) -> bool:
        """Verify that an archived v2 chunk is intact"""
        if chunk_md5 not in self.archived_chunks:
            return False
            
        chunk = self.archived_chunks[chunk_md5]
        chunk_path = Path(chunk.archive_path)
        
        if not chunk_path.exists():
            return False
            
        # Verify hash
        hash_obj = hashlib.md5()
        with open(chunk_path, 'rb') as f:
            for chunk_data in iter(lambda: f.read(8192), b""):
                hash_obj.update(chunk_data)
                
        return hash_obj.hexdigest() == chunk_md5
        
    def verify_blob_integrity(self, depot_manifest: str) -> bool: 
        """Verify that an archived v1 blob is intact"""
        if depot_manifest not in self.archived_blobs:
            return False
            
        blob = self.archived_blobs[depot_manifest]
        blob_path = Path(blob.archive_path)
        
        if not blob_path.exists():
            return False
            
        # Verify file size matches
        actual_size = blob_path.stat().st_size
        return actual_size == blob.total_size
        
    def extract_file_from_blob(self, depot_manifest: str, file_path: str, output_path: str) -> bool:
        """Extract a specific file from a v1 blob"""
        if depot_manifest not in self.archived_blobs:
            return False
            
        blob = self.archived_blobs[depot_manifest]
        blob_path = Path(blob.archive_path)
        
        if not blob_path.exists():
            return False
            
        # Find the file info
        file_info = None
        for file_data in blob.files_contained:
            if file_data['path'] == file_path:
                file_info = file_data
                break
                
        if not file_info:
            return False
            
        try:
            # Extract the file at the specified offset and size
            with open(blob_path, 'rb') as blob_file:
                blob_file.seek(file_info['offset'])
                file_data = blob_file.read(file_info['size'])
                
            with open(output_path, 'wb') as output_file:
                output_file.write(file_data)
                
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to extract {file_path} from blob {depot_manifest}: {e}")
            return False
        
    def get_archive_stats(self) -> Dict:
        """Get statistics about the archive"""
        total_chunks_size = sum(chunk.compressed_size for chunk in self.archived_chunks.values())
        total_blobs_size = sum(blob.total_size for blob in self.archived_blobs.values())
        
        # Count v1 vs v2 builds
        v1_builds = sum(1 for m in self.archived_builds.values() if m.version == 1)
        v2_builds = sum(1 for m in self.archived_builds.values() if m.version == 2)
        
        return {
            'total_builds': len(self.archived_builds),
            'v1_builds': v1_builds,
            'v2_builds': v2_builds, 
            'total_chunks': len(self.archived_chunks),
            'total_blobs': len(self.archived_blobs),
            'chunks_size_bytes': total_chunks_size,
            'blobs_size_bytes': total_blobs_size,
            'total_size_bytes': total_chunks_size + total_blobs_size,
            'total_size_gb': (total_chunks_size + total_blobs_size) / (1024**3),
            'games_archived': len(set(m.game_id for m in self.archived_builds.values())),
            'archive_root': str(self.archive_root)
        }


def main():
    """Example usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='GOG Galaxy CDN Archiver')
    parser.add_argument('--archive-root', required=True, help='Root directory for archive')
    parser.add_argument('--auth-config', help='Path to auth config file')
    parser.add_argument('--game-id', help='Game ID to archive')
    parser.add_argument('--platforms', nargs='+', default=['windows'], help='Platforms to archive')
    parser.add_argument('--stats', action='store_true', help='Show archive statistics')
    parser.add_argument('--verify-chunk', help='Verify integrity of chunk by MD5')
    parser.add_argument('--verify-blob', help='Verify integrity of blob by depot manifest ID')
    parser.add_argument('--extract-file', nargs=3, metavar=('DEPOT_MANIFEST', 'FILE_PATH', 'OUTPUT_PATH'),
                       help='Extract file from v1 blob: depot_manifest file_path output_path')
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format='[%(name)s] %(levelname)s: %(message)s')
    
    archiver = GOGGalaxyArchiver(args.archive_root, args.auth_config)
    
    if args.stats:
        stats = archiver.get_archive_stats()
        print(json.dumps(stats, indent=2))
    elif args.verify_chunk:
        result = archiver.verify_chunk_integrity(args.verify_chunk)
        print(f"Chunk {args.verify_chunk} integrity: {'OK' if result else 'FAILED'}")
    elif args.verify_blob:
        result = archiver.verify_blob_integrity(args.verify_blob)
        print(f"Blob {args.verify_blob} integrity: {'OK' if result else 'FAILED'}")
    elif args.extract_file:
        depot_manifest, file_path, output_path = args.extract_file
        result = archiver.extract_file_from_blob(depot_manifest, file_path, output_path)
        print(f"File extraction: {'SUCCESS' if result else 'FAILED'}")
    elif args.game_id:
        results = archiver.archive_game_complete(args.game_id, args.platforms)
        print(json.dumps(results, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
