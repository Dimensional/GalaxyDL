#!/usr/bin/env python3
"""
GOG Galaxy Archiver - File Extractor

This module provides functionality to extract archived game files from 
V1 blobs (main.bin) and V2 chunks back to their original file structure.
"""

import json
import os
import zlib
from pathlib import Path
import logging
from typing import Dict, List, Optional, Set, Tuple
import hashlib


class GOGArchiveExtractor:
    """Extract files from archived V1 blobs and V2 chunks"""
    
    def __init__(self, archive_root: str, verify_checksums: bool = False):
        """
        Initialize the extractor
        
        Args:
            archive_root: Root directory of the archive
            verify_checksums: Whether to verify file checksums during extraction
        """
        self.archive_root = Path(archive_root)
        self.verify_checksums = verify_checksums
        self.logger = logging.getLogger("EXTRACTOR")
        
        # Validate archive structure
        if not self.archive_root.exists():
            raise FileNotFoundError(f"Archive root does not exist: {archive_root}")
        
        self.blobs_dir = self.archive_root / "blobs"
        self.chunks_dir = self.archive_root / "chunks" 
        self.manifests_dir = self.archive_root / "manifests"
        self.builds_dir = self.archive_root / "builds"
        
        # Load archive database for build mapping
        self.database_path = self.archive_root / "metadata" / "archive_database.json"
        if self.database_path.exists():
            with open(self.database_path, 'r', encoding='utf-8') as f:
                self.database = json.load(f)
        else:
            self.database = {'builds': []}
        
        self.logger.info(f"Initialized extractor for archive: {archive_root}")
        self.logger.info(f"Loaded {len(self.database.get('builds', []))} builds from database")
    
    def extract_build(self, game_id: str, build_id: str, output_dir: str, platform: str = "windows") -> Dict:
        """
        Extract a complete build to the specified output directory
        
        Args:
            game_id: Game ID (e.g., "1207658930")
            build_id: Build ID to extract 
            output_dir: Directory to extract files to
            platform: Platform (windows, osx, linux)
            
        Returns:
            Dictionary with extraction results
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Extracting build {build_id} for game {game_id} to {output_dir}")
        
        if self.verify_checksums:
            self.logger.info("Checksum verification ENABLED - will validate all file and chunk hashes")
        else:
            self.logger.info("Checksum verification DISABLED - extracting without hash validation")
        
        # Load build manifest to determine version
        build_manifest = self._load_build_manifest(game_id, build_id, platform)
        if not build_manifest:
            raise FileNotFoundError(f"Build manifest not found: {game_id}/{build_id}/{platform}")
        
        version = build_manifest.get('version', 2)
        self.logger.info(f"Detected build version: V{version}")
        
        if version == 1:
            return self._extract_v1_build(game_id, build_id, platform, output_path, build_manifest)
        else:
            return self._extract_v2_build(game_id, build_id, platform, output_path, build_manifest)
    
    def _load_build_manifest(self, game_id: str, build_id: str, platform: str) -> Optional[Dict]:
        """Load build manifest from archive using database mapping"""
        
        # Find build in database
        build_info = None
        for build in self.database.get('builds', []):
            if (build['game_id'] == game_id and 
                build['build_id'] == build_id and 
                build['platform'] == platform):
                build_info = build
                break
        
        if not build_info:
            self.logger.error(f"Build not found in database: {game_id}/{build_id}/{platform}")
            return None
        
        self.logger.info(f"Found build in database: V{build_info['version']}, repository_id: {build_info['repository_id']}")
        
        # Load build manifest using archive_path from database
        build_manifest_path = self.archive_root / build_info['archive_path']
        
        if not build_manifest_path.exists():
            self.logger.error(f"Build manifest file not found: {build_manifest_path}")
            return None
        
        try:
            if build_info['version'] == 1:
                # V1 manifests are plain JSON
                with open(build_manifest_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # V2 manifests are zlib compressed JSON
                with open(build_manifest_path, 'rb') as f:
                    compressed_data = f.read()
                
                decompressed_data = zlib.decompress(compressed_data)
                return json.loads(decompressed_data.decode('utf-8'))
                
        except Exception as e:
            self.logger.error(f"Error loading build manifest from {build_manifest_path}: {e}")
            return None
    
    def _extract_v1_build(self, game_id: str, build_id: str, platform: str, output_path: Path, build_manifest: Dict) -> Dict:
        """Extract V1 build from blobs"""
        self.logger.info("Extracting V1 build from blobs")
        
        # Get build info from database to find repository_id
        build_info = None
        for build in self.database.get('builds', []):
            if (build['game_id'] == game_id and 
                build['build_id'] == build_id and 
                build['platform'] == platform):
                build_info = build
                break
        
        if not build_info:
            raise ValueError(f"Build not found in database: {game_id}/{build_id}/{platform}")
        
        repository_id = build_info['repository_id']
        self.logger.info(f"Using repository_id: {repository_id}")
        
        results = {
            'version': 1,
            'files_extracted': 0,
            'total_size': 0,
            'errors': []
        }
        
        # Get depot manifests and collect all files
        product = build_manifest.get('product', {})
        depots = product.get('depots', [])
        
        all_files = []  # List to store all files from all manifests
        
        for depot in depots:
            if 'manifest' not in depot:
                continue  # Skip redist entries
                
            manifest_id = depot['manifest']
            self.logger.info(f"Processing V1 depot manifest: {manifest_id}")
            
            # Load depot manifest using repository_id
            # Note: manifest_id already includes .json extension for V1
            depot_manifest_path = (self.manifests_dir / "v1" / "manifests" / game_id / platform / repository_id / manifest_id)
            
            if not depot_manifest_path.exists():
                self.logger.warning(f"Depot manifest not found: {depot_manifest_path}")
                continue
            
            try:
                with open(depot_manifest_path, 'r', encoding='utf-8') as f:
                    depot_manifest = json.load(f)
                
                # Extract files from this depot and add to collection
                depot_data = depot_manifest.get('depot', {})
                files = depot_data.get('files', [])
                
                for file_info in files:
                    # Fix path by removing leading slash if present
                    file_path = file_info['path']
                    if file_path.startswith('/'):
                        file_path = file_path[1:]  # Remove leading slash
                        file_info = file_info.copy()  # Don't modify original
                        file_info['path'] = file_path
                    
                    all_files.append(file_info)
                    
                self.logger.info(f"Added {len(files)} files from manifest {manifest_id}")
                
            except Exception as e:
                error_msg = f"Error processing depot {manifest_id}: {e}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Sort all files by offset for efficient extraction
        all_files.sort(key=lambda f: f.get('offset', 0))
        self.logger.info(f"Total files to extract: {len(all_files)}, sorted by offset")
        
        # Extract all files in offset order
        extraction_results = self._extract_v1_files_sorted(game_id, build_id, repository_id, all_files, output_path)
        results['files_extracted'] += extraction_results['files_extracted']
        results['total_size'] += extraction_results['total_size']
        results['errors'].extend(extraction_results['errors'])
        
        self.logger.info(f"V1 extraction complete: {results['files_extracted']} files, {results['total_size']:,} bytes")
        return results
    
    def _extract_v1_files_sorted(self, game_id: str, repository_id: str, all_files: List[Dict], output_path: Path) -> Dict:
        """Extract V1 files sorted by offset for efficient sequential reading"""
        results = {
            'files_extracted': 0,
            'total_size': 0,
            'errors': []
        }
        
        if not all_files:
            return results
        
        # Find blob file
        blob_path = self.blobs_dir / repository_id / "main.bin"
        if not blob_path.exists():
            error_msg = f"Blob file not found: {blob_path}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
        
        self.logger.info(f"Extracting {len(all_files)} files from {blob_path}")
        
        # Open blob file once and extract all files sequentially
        try:
            with open(blob_path, 'rb') as blob_file:
                for file_info in all_files:
                    try:
                        file_path = file_info['path']
                        file_size = file_info['size']
                        file_hash = file_info['hash']
                        blob_offset = file_info['offset']
                        
                        self.logger.debug(f"Extracting: {file_path} ({file_size:,} bytes at offset {blob_offset})")
                        
                        # Read file data
                        blob_file.seek(blob_offset)
                        file_data = blob_file.read(file_size)
                        
                        # Verify hash
                        calculated_hash = hashlib.md5(file_data).hexdigest()
                        if calculated_hash != file_hash:
                            error_msg = f"Hash mismatch for {file_path}: expected {file_hash}, got {calculated_hash}"
                            self.logger.error(error_msg)
                            results['errors'].append(error_msg)
                            continue
                        
                        # Write to output
                        output_file_path = output_path / file_path
                        output_file_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(output_file_path, 'wb') as output_file:
                            output_file.write(file_data)
                        
                        results['files_extracted'] += 1
                        results['total_size'] += file_size
                        
                    except Exception as e:
                        error_msg = f"Error extracting {file_info.get('path', 'unknown')}: {e}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        
        except Exception as e:
            error_msg = f"Error opening blob file {blob_path}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
        
        return results
    
    def _extract_v1_files_sorted(self, game_id: str, build_id: str, repository_id: str, all_files: List[Dict], output_path: Path) -> Dict:
        """Extract V1 files in offset order for efficient blob reading"""
        results = {
            'files_extracted': 0,
            'total_size': 0,
            'errors': []
        }
        
        if not all_files:
            return results
        
        # Find blob file - try build_id first, then fall back to game_id for backwards compatibility
        blob_path_by_build = self.blobs_dir / build_id / "main.bin"
        blob_path_by_game = self.blobs_dir / game_id / "main.bin"
        
        blob_path = None
        if blob_path_by_build.exists():
            blob_path = blob_path_by_build
            self.logger.info(f"Using blob with build_id path: {blob_path}")
        elif blob_path_by_game.exists():
            blob_path = blob_path_by_game
            self.logger.info(f"Using blob with game_id path (legacy): {blob_path}")
        else:
            error_msg = f"Blob file not found in either location: {blob_path_by_build} or {blob_path_by_game}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
        
        try:
            with open(blob_path, 'rb') as blob_file:
                for file_info in all_files:
                    try:
                        file_path = file_info['path']
                        file_size = file_info['size']
                        file_hash = file_info['hash']
                        blob_offset = file_info['offset']
                        
                        self.logger.debug(f"Extracting: {file_path} ({file_size:,} bytes) at offset {blob_offset}")
                        
                        # Read file data from blob
                        blob_file.seek(blob_offset)
                        file_data = blob_file.read(file_size)
                        
                        if len(file_data) != file_size:
                            error_msg = f"Read size mismatch for {file_path}: expected {file_size}, got {len(file_data)}"
                            self.logger.error(error_msg)
                            results['errors'].append(error_msg)
                            continue
                        
                        # Verify hash if checksums validation is enabled
                        if self.verify_checksums:
                            calculated_hash = hashlib.md5(file_data).hexdigest()
                            if calculated_hash != file_hash:
                                error_msg = f"Hash mismatch for {file_path}: expected {file_hash}, got {calculated_hash}"
                                self.logger.error(error_msg)
                                results['errors'].append(error_msg)
                                continue
                        
                        # Write to output
                        output_file_path = output_path / file_path
                        output_file_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(output_file_path, 'wb') as output_file:
                            output_file.write(file_data)
                        
                        results['files_extracted'] += 1
                        results['total_size'] += file_size
                        
                    except Exception as e:
                        error_msg = f"Error extracting {file_info.get('path', 'unknown')}: {e}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
        
        except Exception as e:
            error_msg = f"Error reading blob file {blob_path}: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
        
        return results

    def _extract_v1_depot(self, game_id: str, depot_manifest: Dict, output_path: Path) -> Dict:
        """Extract files from a V1 depot"""
        results = {
            'files_extracted': 0,
            'total_size': 0,
            'errors': []
        }
        
        depot_data = depot_manifest.get('depot', {})
        files = depot_data.get('files', [])
        
        for file_info in files:
            try:
                # Extract file path and properties
                file_path = file_info['path']
                file_size = file_info['size']
                file_hash = file_info['hash']
                blob_url = file_info['url']  # e.g., "1207658930/main.bin"
                blob_offset = file_info['offset']
                
                self.logger.debug(f"Extracting: {file_path} ({file_size:,} bytes)")
                
                # Read from blob
                blob_path = self.blobs_dir / blob_url
                if not blob_path.exists():
                    error_msg = f"Blob not found: {blob_path}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
                    continue
                
                # Extract file data
                with open(blob_path, 'rb') as blob_file:
                    blob_file.seek(blob_offset)
                    file_data = blob_file.read(file_size)
                
                # Verify hash if checksums validation is enabled
                if self.verify_checksums:
                    calculated_hash = hashlib.md5(file_data).hexdigest()
                    if calculated_hash != file_hash:
                        error_msg = f"Hash mismatch for {file_path}: expected {file_hash}, got {calculated_hash}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        continue
                
                # Write to output
                output_file_path = output_path / file_path
                output_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file_path, 'wb') as output_file:
                    output_file.write(file_data)
                
                results['files_extracted'] += 1
                results['total_size'] += file_size
                
            except Exception as e:
                error_msg = f"Error extracting {file_info.get('path', 'unknown')}: {e}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
        
        return results
    
    def _extract_v2_build(self, game_id: str, build_id: str, platform: str, output_path: Path, build_manifest: Dict) -> Dict:
        """Extract V2 build from chunks"""
        self.logger.info("Extracting V2 build from chunks")
        
        results = {
            'version': 2,
            'files_extracted': 0,
            'total_size': 0,
            'errors': []
        }
        
        # Get depot manifests  
        depots = build_manifest.get('depots', [])
        
        for depot in depots:
            manifest_id = depot['manifest']
            self.logger.info(f"Processing V2 depot manifest: {manifest_id}")
            
            # Load depot manifest
            manifest_path = self._find_v2_manifest(manifest_id)
            if not manifest_path:
                self.logger.warning(f"V2 depot manifest not found: {manifest_id}")
                continue
            
            try:
                with open(manifest_path, 'rb') as f:
                    compressed_data = f.read()
                
                # Decompress zlib data
                decompressed_data = zlib.decompress(compressed_data)
                depot_manifest = json.loads(decompressed_data.decode('utf-8'))
                
                # Extract files from this depot
                depot_results = self._extract_v2_depot(depot_manifest, output_path)
                results['files_extracted'] += depot_results['files_extracted']
                results['total_size'] += depot_results['total_size']
                results['errors'].extend(depot_results['errors'])
                
            except Exception as e:
                error_msg = f"Error processing V2 depot {manifest_id}: {e}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
        
        self.logger.info(f"V2 extraction complete: {results['files_extracted']} files, {results['total_size']:,} bytes")
        return results
    
    def _find_v2_manifest(self, manifest_id: str) -> Optional[Path]:
        """Find V2 manifest file by ID (zlib compressed files, not .json)"""
        # Use galaxy path format: ab/cd/abcd1234...
        if len(manifest_id) < 4:
            return None
            
        galaxy_path = f"{manifest_id[:2]}/{manifest_id[2:4]}/{manifest_id}" 
        manifest_path = self.manifests_dir / "v2" / "meta" / galaxy_path
        
        if manifest_path.exists():
            return manifest_path
        
        return None
    
    def _extract_v2_depot(self, depot_manifest: Dict, output_path: Path) -> Dict:
        """Extract files from a V2 depot"""
        results = {
            'files_extracted': 0,
            'total_size': 0,
            'errors': []
        }
        
        depot_data = depot_manifest.get('depot', {})
        items = depot_data.get('items', [])
        
        for item in items:
            if item.get('type') != 'DepotFile':
                continue  # Skip directories and links for now
            
            try:
                file_path = item['path']
                chunks = item['chunks']
                
                self.logger.debug(f"Extracting: {file_path} ({len(chunks)} chunks)")
                
                # Reconstruct file from chunks
                file_data = b''
                total_uncompressed_size = 0
                
                for chunk in chunks:
                    chunk_hash = chunk['compressedMd5']
                    compressed_size = chunk['compressedSize']
                    uncompressed_size = chunk['size']
                    expected_md5 = chunk['md5']
                    
                    # Read chunk
                    chunk_data = self._read_chunk(chunk_hash)
                    if chunk_data is None:
                        error_msg = f"Chunk not found: {chunk_hash}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        break
                    
                    # Verify compressed chunk data matches filename (pre-extraction validation)
                    if self.verify_checksums:
                        calculated_chunk_md5 = hashlib.md5(chunk_data).hexdigest()
                        if calculated_chunk_md5 != chunk_hash:
                            error_msg = f"Compressed chunk hash mismatch for {chunk_hash}: expected {chunk_hash}, got {calculated_chunk_md5}"
                            self.logger.error(error_msg)
                            results['errors'].append(error_msg)
                            break
                        else:
                            self.logger.debug(f"✓ Compressed chunk validation passed for {chunk_hash}")
                    
                    # Verify compressed size
                    if len(chunk_data) != compressed_size:
                        error_msg = f"Chunk size mismatch for {chunk_hash}: expected {compressed_size}, got {len(chunk_data)}"
                        self.logger.warning(error_msg)
                    
                    # Decompress chunk
                    try:
                        decompressed_data = zlib.decompress(chunk_data)
                    except zlib.error as e:
                        error_msg = f"Failed to decompress chunk {chunk_hash}: {e}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        break
                    
                    # Verify decompressed size if checksums validation is enabled
                    if self.verify_checksums and len(decompressed_data) != uncompressed_size:
                        error_msg = f"Decompressed size mismatch for {chunk_hash}: expected {uncompressed_size}, got {len(decompressed_data)}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        break
                    
                    # Verify MD5 of decompressed data (post-decompression validation)
                    if self.verify_checksums:
                        calculated_md5 = hashlib.md5(decompressed_data).hexdigest()
                        if calculated_md5 != expected_md5:
                            error_msg = f"Decompressed data MD5 mismatch for chunk {chunk_hash}: expected {expected_md5}, got {calculated_md5}"
                            self.logger.error(error_msg)
                            results['errors'].append(error_msg)
                            break
                        else:
                            self.logger.debug(f"✓ Decompressed data validation passed for {chunk_hash}")
                    
                    file_data += decompressed_data
                    total_uncompressed_size += uncompressed_size
                
                else:
                    # All chunks processed successfully
                    # Write to output
                    output_file_path = output_path / file_path
                    output_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    with open(output_file_path, 'wb') as output_file:
                        output_file.write(file_data)
                    
                    results['files_extracted'] += 1
                    results['total_size'] += total_uncompressed_size
                    
            except Exception as e:
                error_msg = f"Error extracting {item.get('path', 'unknown')}: {e}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
        
        return results
    
    def _read_chunk(self, chunk_hash: str) -> Optional[bytes]:
        """Read chunk data from archive"""
        if len(chunk_hash) < 4:
            return None
        
        # Use galaxy path format: ab/cd/abcd1234...
        galaxy_path = f"{chunk_hash[:2]}/{chunk_hash[2:4]}/{chunk_hash}"
        chunk_path = self.chunks_dir / galaxy_path
        
        if not chunk_path.exists():
            return None
        
        try:
            with open(chunk_path, 'rb') as chunk_file:
                return chunk_file.read()
        except Exception as e:
            self.logger.error(f"Error reading chunk {chunk_hash}: {e}")
            return None
    
    def list_available_builds(self) -> List[Dict]:
        """List all builds available for extraction from database"""
        builds = []
        
        for build in self.database.get('builds', []):
            # Check if the build manifest file exists
            build_manifest_path = self.archive_root / build['archive_path']
            if build_manifest_path.exists():
                builds.append({
                    'version': build['version'],
                    'game_id': build['game_id'],
                    'build_id': build['build_id'],
                    'platform': build['platform'],
                    'repository_id': build['repository_id'],
                    'version_name': build.get('version_name', ''),
                    'tags': build.get('tags', []),
                    'path': str(build_manifest_path)
                })
            else:
                self.logger.warning(f"Build manifest missing: {build_manifest_path}")
        
        return builds
