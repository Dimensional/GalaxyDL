# GalaxyDL - GOG Galaxy Downloader and Archiver

A .NET implementation of the GOG Galaxy content downloader and archiver, providing similar functionality to DepotDownloader for Steam. GalaxyDL can archive game manifests and content from GOG Galaxy's content delivery system.

## Features

- ?? **Automatic OAuth2 Authentication** - Browser-based login flow
- ?? **Build Discovery** - List available builds for any GOG game
- ?? **Manifest Archiving** - Download and preserve build and depot manifests
- ??? **Archive Management** - Organized storage with file system truth validation
- ?? **V1 & V2 Support** - Handle both legacy and modern GOG APIs
- ?? **Cross-Platform** - Runs on Windows, macOS, and Linux

## Quick Start

### 1. Authentication

GalaxyDL uses OAuth2 for secure authentication with GOG:

```bash
# Authenticate with GOG (opens browser automatically)
dotnet run -- auth login

# Check authentication status
dotnet run -- auth status
```

The authentication process:
1. Opens your default browser to GOG's login page
2. You log in with your GOG credentials
3. GOG redirects back to GalaxyDL with an authorization code
4. GalaxyDL exchanges the code for access tokens
5. Credentials are saved securely to `auth.json`

### 2. List Game Builds

```bash
# List builds for The Witcher 2
dotnet run -- archive list-builds --game-id 1207658930

# List builds for a specific platform
dotnet run -- archive list-builds --game-id 1207658930 --platforms windows

# List only V1 or V2 builds
dotnet run -- archive list-builds --game-id 1207658930 --generation 2
```

### 3. Archive Manifests

```bash
# Archive build and depot manifests (no content download)
dotnet run -- archive archive-manifests --game-id 1207658930 --build-id <build_id> --archive-root ./archive

# Example with a real build ID
dotnet run -- archive archive-manifests --game-id 1207658930 --build-id 55136646768941159 --archive-root ./archive
```

### 4. View Archive Statistics

```bash
# Show archive statistics
dotnet run -- archive stats --archive-root ./archive
```

## Command Reference

### Authentication Commands

| Command | Description |
|---------|-------------|
| `auth login` | Start OAuth2 authentication flow |
| `auth status` | Check current authentication status |

### Archive Commands

| Command | Description |
|---------|-------------|
| `archive test` | Test application functionality |
| `archive list-builds` | List available builds for a game |
| `archive archive-manifests` | Archive build and depot manifests |
| `archive stats` | Show archive statistics |

### Common Options

| Option | Description | Default |
|--------|-------------|---------|
| `--auth-config` | Path to authentication config file | `./auth.json` |
| `--game-id` | GOG Game ID (required for game operations) | - |
| `--build-id` | Specific build ID to archive | - |
| `--archive-root` | Root directory for archive storage | - |
| `--platforms` | Platforms to query (windows, osx, linux) | `windows` |
| `--generation` | API generation (1 or 2) | Both |

## Finding Game IDs

You can find GOG Game IDs by:

1. **GOG Store URL**: The ID is in the URL: `https://www.gog.com/game/[game_name]_[GAME_ID]`
2. **GOG Galaxy**: Inspect network requests in browser developer tools
3. **Common Examples**:
   - The Witcher 2: `1207658930`
   - Cyberpunk 2077: `1423049311`
   - The Witcher 3: `1640424747`

## Archive Structure

GalaxyDL creates a well-organized archive structure:

```
archive/
??? builds/           # Build manifests
?   ??? v1/          # V1 (legacy) manifests
?   ??? v2/          # V2 (modern) manifests
??? manifests/        # Depot manifests
?   ??? v1/          # V1 depot manifests
?   ??? v2/          # V2 depot manifests
??? chunks/           # V2 content chunks (future)
??? blobs/            # V1 content blobs (future)
??? metadata/         # Archive database and metadata
    ??? database.json # Archive tracking database
```

## Authentication File

The `auth.json` file stores your GOG credentials securely:

```json
{
  "46899977096215655": {
    "access_token": "...",
    "refresh_token": "...",
    "expires_in": 3600,
    "token_type": "bearer",
    "scope": "user.profile user.library",
    "login_time": 1640995200
  }
}
```

**Keep this file secure** - it contains your GOG access tokens.

## Current Status

### ? Implemented Features

- **Full OAuth2 Authentication** with automatic browser flow
- **Build Discovery** for V1 and V2 APIs
- **Manifest Archiving** (build manifests and depot manifests)
- **Archive Management** with database tracking
- **Cross-Platform Support** (Windows, macOS, Linux)

### ?? In Development

- **Content Downloading** (chunks and blobs)
- **File Extraction** from archives
- **Comprehensive Validation** of downloaded content

### ?? Planned Features

- **Progress Reporting** for downloads
- **Bandwidth Throttling** options
- **Batch Operations** for multiple games
- **Archive Validation** tools

## Configuration

You can customize GalaxyDL behavior via `appsettings.json`:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "GalaxyDL": "Debug"
    }
  },
  "GogApi": {
    "DefaultTimeout": 30,
    "DownloadTimeout": 300,
    "MaxRetries": 3,
    "ChunkSize": 104857600
  },
  "Archive": {
    "DefaultArchiveRoot": "./archive",
    "MaxConcurrentDownloads": 4,
    "EnableIntegrityChecking": true,
    "SavePrettifiedManifests": true
  }
}
```

## Requirements

- .NET 9.0 or later
- Internet connection for GOG API access
- GOG account with owned games

## Troubleshooting

### Authentication Issues

1. **Port 8080 in use**: GalaxyDL uses port 8080 for OAuth callback. Ensure it's available.
2. **Browser doesn't open**: Manually navigate to the URL displayed in the console.
3. **Token expired**: Run `dotnet run -- auth login` to re-authenticate.

### API Issues

1. **Rate limiting**: GOG may rate limit requests. Wait a few minutes and try again.
2. **Game not found**: Verify the Game ID is correct.
3. **No builds found**: Some games may not have public builds available.

### General Issues

1. **Check logs**: Look in the `logs/` directory for detailed error information.
2. **Update credentials**: Re-run `dotnet run -- auth login` if you encounter authentication errors.

## Contributing

GalaxyDL is under active development. The core manifest archiving functionality is complete and working. The next major milestone is implementing content downloading (chunks and blobs).

## License

This project is for educational and archival purposes. Please respect GOG's Terms of Service and only download content you own.

---

**Note**: GalaxyDL is currently in active development. The manifest archiving functionality is complete and ready for use. Content downloading features are planned for future releases.