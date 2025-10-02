namespace GalaxyDL.Core;

/// <summary>
/// Constants for GOG API endpoints and configuration
/// </summary>
public static class GogConstants
{
    // API Endpoints
    public const string GOG_CDN = "https://gog-cdn-fastly.gog.com";
    public const string GOG_CDN_ALT = "https://cdn.gog.com"; // Alternative CDN for some endpoints
    public const string GOG_CONTENT_SYSTEM = "https://content-system.gog.com";
    public const string GOG_MANIFESTS_COLLECTOR = "https://downloadable-manifests-collector.gog.com";
    public const string GOG_EMBED = "https://embed.gog.com";
    public const string GOG_AUTH = "https://auth.gog.com";
    public const string GOG_API = "https://api.gog.com";

    // OAuth2 Configuration
    public const string CLIENT_ID = "46899977096215655";
    public const string CLIENT_SECRET = "9d85c43b1482497dbbce61f6e4aa173a433796eeae2ca8c5f6129f2dc4de46d9";
    public const string CODE_URL = "https://auth.gog.com/token?client_id=46899977096215655&client_secret=9d85c43b1482497dbbce61f6e4aa173a433796eeae2ca8c5f6129f2dc4de46d9&grant_type=authorization_code&redirect_uri=https%3A%2F%2Fembed.gog.com%2Fon_login_success%3Forigin%3Dclient&code=";

    // Dependencies URLs (from Python constants)
    public const string DEPENDENCIES_URL = "https://gog-cdn-fastly.gog.com/content-system/v2/dependencies/repository";
    public const string DEPENDENCIES_V1_URL = "https://gog-cdn-fastly.gog.com/content-system/v1/dependencies/repository";

    // User Agent
    public const string USER_AGENT_BASE = "gogdl";
    public const string HEROIC_REFERENCE = "(Heroic Games Launcher)";

    // Archive Structure
    public static class ArchiveStructure
    {
        public const string BUILDS_DIR = "builds";
        public const string MANIFESTS_DIR = "manifests";
        public const string CHUNKS_DIR = "chunks";
        public const string BLOBS_DIR = "blobs";
        public const string METADATA_DIR = "metadata";
        public const string DATABASE_FILE = "archive_database.json";

        // Version specific paths
        public const string V1_MANIFESTS = "v1/manifests";
        public const string V2_META = "v2/meta";
        public const string V2_BUILDS = "v2/builds";
        public const string V2_DEPOTS = "v2/depots";
    }

    // Download Configuration
    public static class Download
    {
        public const int DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024; // 100 MiB chunks
        public const int DEFAULT_MAX_WORKERS = 4;
        public const int DEFAULT_TIMEOUT_SECONDS = 30;
        public const int DOWNLOAD_TIMEOUT_SECONDS = 300;
        public const int BUFFER_SIZE = 65536; // 64KB
    }

    // Validation Configuration
    public static class Validation
    {
        public const string MD5_ALGORITHM = "MD5";
        public const string SHA1_ALGORITHM = "SHA1";
        public const string SHA256_ALGORITHM = "SHA256";
    }

    // Platform identifiers
    public static class Platforms
    {
        public const string WINDOWS = "windows";
        public const string OSX = "osx";
        public const string LINUX = "linux";
        
        public static readonly string[] ALL_PLATFORMS = { WINDOWS, OSX, LINUX };
        
        // Common platform combinations for different use cases
        public static readonly string[] COMMON_PLATFORMS = { WINDOWS, OSX }; // Most common GOG platforms
        public static readonly string[] DESKTOP_PLATFORMS = { WINDOWS, OSX, LINUX }; // All desktop platforms
        
        // Platform display names
        public static readonly Dictionary<string, string> PLATFORM_NAMES = new()
        {
            [WINDOWS] = "Windows",
            [OSX] = "macOS", 
            [LINUX] = "Linux"
        };
        
        // Platform symbols for display
        public static readonly Dictionary<string, string> PLATFORM_SYMBOLS = new()
        {
            [WINDOWS] = "??",
            [OSX] = "??",
            [LINUX] = "??"
        };
    }

    // Generation identifiers
    public static class Generations
    {
        public const int V1 = 1;
        public const int V2 = 2;
    }

    // File extensions and signatures
    public static class FileSignatures
    {
        public static readonly byte[] GZIP_SIGNATURE = { 0x1f, 0x8b };
        public static readonly byte[] ZLIB_SIGNATURE = { 0x78 };
    }

    // HTTP Headers
    public static class Headers
    {
        public const string AUTHORIZATION = "Authorization";
        public const string USER_AGENT = "User-Agent";
        public const string CONTENT_LENGTH = "Content-Length";
        public const string RANGE = "Range";
        public const string ACCEPT_ENCODING = "Accept-Encoding";
    }

    // Authentication token fields
    public static class AuthTokenFields
    {
        public const string ACCESS_TOKEN = "access_token";
        public const string REFRESH_TOKEN = "refresh_token";
        public const string EXPIRES_IN = "expires_in";
        public const string LOGIN_TIME = "loginTime";
        public const string TOKEN_TYPE = "token_type";
    }

    // URL Patterns for different manifest types
    public static class UrlPatterns
    {
        // V1 patterns
        public const string V1_BUILDS_LIST = "{0}/content-system/v1/meta/{1}/{2}"; // CDN, gameId, platform
        public const string V1_BUILD_MANIFEST = "{0}/content-system/v1/manifests/{1}/{2}/{3}/repository.json"; // CDN, gameId, platform, repositoryId
        public const string V1_DEPOT_MANIFEST = "{0}/content-system/v1/manifests/{1}/{2}/{3}/{4}"; // CDN, gameId, platform, buildId, manifestId
        
        // V2 patterns  
        public const string V2_BUILDS_LIST = "{0}/products/{1}/os/{2}/builds"; // content-system, gameId, platform
        public const string V2_BUILD_MANIFEST = "{0}/content-system/v2/meta/{1}"; // CDN, repositoryPath (xx/yy/repositoryId)
        public const string V2_DEPOT_MANIFEST = "{0}/manifests/depots/{1}"; // manifests-collector, galaxyPath
    }
}