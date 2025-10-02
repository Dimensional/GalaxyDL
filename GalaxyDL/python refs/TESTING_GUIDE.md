# Testing the GOG Galaxy Archiver with The Witcher 2

## Test Plan

Based on the GOG database information for The Witcher 2 (ID: 1207658930), this game is perfect for testing our archiver because it has both v1 and v2 manifests.

## Authentication Setup

There are several ways to authenticate with GOG for testing:

### Method 1: Using GOG Galaxy ~~(Easiest)~~ (Does not work)
If you have GOG Galaxy installed, you can copy its authentication:

1. **Find GOG Galaxy's auth file:**
   - Windows: `%LOCALAPPDATA%\GOG.com\Galaxy\Configuration\auth.json`
   - Linux: `~/.local/share/gog/Galaxy/Configuration/auth.json`
   - macOS: `~/Library/Application Support/GOG.com/Galaxy/Configuration/auth.json`

2. **Copy the auth file:**
   ```bash
   cp "/path/to/gog/galaxy/auth.json" "./gog_auth.json"
   ```

### Method 2: Built-in gogdl OAuth2 (Recommended)
If you don't have GOG Galaxy or prefer fresh authentication:

1. **Visit the official OAuth2 URL:**
   ```
   https://auth.gog.com/auth?client_id=46899977096215655&redirect_uri=https%3A%2F%2Fembed.gog.com%2Fon_login_success%3Forigin%3Dclient&response_type=code&layout=client2
   ```

2. **Log in and get the code:**
   - Log in with your GOG account
   - After successful login, you'll be redirected to a URL like:
   ```
   https://embed.gog.com/on_login_success?origin=client&code=YOUR_CODE_HERE
   ```
   - **IMPORTANT**: Copy the `code` parameter immediately - it expires within 10 minutes!

3. **Use gogdl's built-in auth command (run immediately):**
   ```bash
   python -m gogdl.cli auth --auth-config-path "gog_auth.json" --code YOUR_CODE_HERE
   ```

   **Notes:**
   - The auth file (`gog_auth.json`) will be created if it doesn't exist
   - Authorization codes are single-use and expire quickly
   - If you get `{"error": true}`, the code has likely expired - get a new one
   - Success will show JSON token data instead of an error

### Method 3: Existing Heroic Installation
If you use Heroic Games Launcher:

1. **Find Heroic's auth file:**
   - Windows: `%APPDATA%\heroic\gog_store\auth.json`
   - Linux: `~/.config/heroic/gog_store/auth.json`
   - macOS: `~/Library/Application Support/heroic/gog_store/auth.json`

2. **Copy the auth file:**
   ```bash
   cp "/path/to/heroic/gog_store/auth.json" "./gog_auth.json"
   ```

## Troubleshooting Authentication

### Common Issues:

1. **`{"error": true}` when using OAuth2 code:**
   - The authorization code has expired (they expire in ~10 minutes)
   - The code has already been used (single-use only)
   - Solution: Get a fresh code and use it immediately

2. **"No such file or directory" errors:**
   - Make sure you're in the correct directory
   - Check that the auth file paths exist for your system

3. **Authentication file format:**
   - The auth file should be valid JSON
   - It should contain fields like `access_token`, `refresh_token`, `expires_in`
   - gogdl will automatically refresh expired tokens if it has a valid `refresh_token`

### Testing Authentication:
Once you have `gog_auth.json`, test it with:
```bash
python -m gogdl.cli auth --auth-config-path "gog_auth.json"
```
This should show your current token info (not an error).