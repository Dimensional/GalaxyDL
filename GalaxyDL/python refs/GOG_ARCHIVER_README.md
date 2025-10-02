# GOG Galaxy CDN Archiver - Implementation Summary

## Overview
This archiver collects and preserves GOG Galaxy's CDN content similar to SteamArchiver_Python, but handles GOG's unique dual-manifest system.

Initial setup for this should be followed by using the [TESTING_GUIDE.md](TESTING_GUIDE.md)

## Key Architecture Differences

### v1 Manifests (Legacy - "main.bin" System)
- **Structure**: Each depot contains ONE large `main.bin` file
- **File Access**: Files are extracted using offset/size within the single binary blob
- **URL Pattern**: `{secure_cdn_url}/main.bin`
- **Storage**: Blobs stored as `blobs/{build_id}/main.bin`
- **Identification**: Uses depot manifest hash as identifier

### v2 Manifests (Modern - Chunk System)
- **Structure**: Files split into individual MD5-named chunks (similar to Steam's SHA1 chunks)
- **File Access**: Files reconstructed from multiple chunks
- **URL Pattern**: `{secure_cdn_url}/{chunk_md5[:2]}/{chunk_md5[2:4]}/{chunk_md5}`
- **Storage**: Chunks stored as `chunks/{md5[:2]}/{md5[2:4]}/{md5}`
- **Identification**: Uses MD5 hash as identifier

## Corrected Implementation Details

### 1. v1 Blob Handling
```python
# Each depot has exactly ONE main.bin file
# URL construction matches existing gogdl v1 downloader:
endpoint["parameters"]["path"] += "/main.bin"
url = merge_url_with_params(endpoint["url_format"], endpoint["parameters"])

# Files are stored with their offset/size metadata for extraction:
{
    'path': 'game.exe',
    'offset': 1024,
    'size': 5242880,
    'hash': 'file_checksum'
}
```

### 2. v2 Chunk Handling  
```python
# Individual chunks with MD5 names
# URL construction uses galaxy_path() function:
chunk_path = galaxy_path(chunk_md5)  # "ab/cd/abcdef..."
url = f"{secure_cdn_url}/{chunk_path}"
```

### 3. Archive Directory Structure
```
archive_root/
├── manifests/           # JSON manifests (both v1 and v2)
│   │
│   ├── v1/
|   |   └── manifests/
|   |               └── {game_id}/
|   |                         └── {platform}/
|   |                                     └── {build_id}/
|   |                                                 └── {manifest id}.json
│   └── v2/
|        └── meta/
|              └── {repository_id[:2]}/
|                            └── {repository_id[2:4]}/
|                                           └── {repository_id}
├── chunks/              # v2 MD5-named chunks
│   └── {md5[:2]}/
│       └── {md5[2:4]}/
│           └── {md5}
├── blobs/               # v1 main.bin files
│   └── {build_id}/
│           └── main.bin
├── builds/
|       ├── v1/
|       |    └── manifests/
|       |              └── {game_id}/
|       |                       └── {platform}/
|       |                                 └── {repository_id}/
|       |                                                 ├── repository.json # Downloaded
|       |                                                 └── repository.json # Prettified version
|       └── v2/
|            └── meta/
|                   └── {repository_id[:2]}/
|                                   └── {repository_id[2:4]}/
|                                                   ├── {repository_id} # zlib compressed json manifest
|                                                   └── {repository_id}.json # decompressed json manifest
├── metadata/            # Additional metadata
└── archive_database.json # Tracking database
```

## Usage Examples

### Archive a Game
```bash
# CLI usage
python -m gogdl.cli archive 1234567890 \
  --auth-config-path auth.json \
  --archive-root ./gog_archive \
  --platforms windows linux

# Standalone usage  
python -m gogdl.archiver \
  --archive-root ./gog_archive \
  --auth-config auth.json \
  --game-id 1234567890
```

### IGNORE THE Following Examples ###
### Extract Files from v1 Blobs
```bash
python -m gogdl.archiver \
  --archive-root ./gog_archive \
  --extract-file depot_manifest_hash "game.exe" "./extracted_game.exe"
```

### Verify Archive Integrity
```bash
# Verify v2 chunk
python -m gogdl.archiver \
  --archive-root ./gog_archive \
  --verify-chunk abc123def456...

# Verify v1 blob
python -m gogdl.archiver \
  --archive-root ./gog_archive \
  --verify-blob depot_manifest_hash
```

## Key Technical Points

1. **Authentication**: Uses GOG's OAuth2 system via existing auth manager
2. **Secure Links**: Uses GOG's secure link API for temporary CDN URLs
3. **Concurrent Downloads**: Multi-threaded downloads with proper error handling
4. **Integrity Verification**: MD5 verification for chunks, size verification for blobs
5. **Deduplication**: Tracks downloaded content to prevent re-downloading
6. **Cross-Platform**: Handles Windows, macOS, and Linux builds

## Comparison with Steam Archiver

| Feature | GOG v1 | GOG v2 | Steam |
|---------|--------|--------|--------|
| Content Units | Binary blobs | MD5 chunks | SHA1 chunks |
| File Access | Offset/size | Chunk reconstruction | Chunk reconstruction |
| URL Pattern | `/main.bin` | `/{md5_path}` | `/{sha1_path}` |
| Auth Method | OAuth2 | OAuth2 | Session-based |
| Compression | None | zlib | Various |

This implementation successfully handles both GOG's legacy and modern content delivery systems, providing a complete archival solution for GOG Galaxy CDN content.
