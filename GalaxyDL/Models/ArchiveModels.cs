using System.Text.Json.Serialization;

namespace GalaxyDL.Models;

/// <summary>
/// Represents an archived chunk/blob from v2 manifests
/// </summary>
public record ArchivedChunk(
    string Md5,
    string? Sha256,
    long CompressedSize,
    string ArchivePath,
    string CdnPath,
    DateTime FirstSeen,
    DateTime LastVerified
);

/// <summary>
/// Represents an archived binary blob from v1 manifests (main.bin files)
/// </summary>
public record ArchivedBlob(
    string DepotManifest,
    string SecureUrl,
    long TotalSize,
    string ArchivePath,
    DateTime FirstSeen,
    DateTime LastVerified,
    List<Dictionary<string, object>>? FilesContained = null,
    Dictionary<string, object>? DepotInfo = null
);

/// <summary>
/// Represents an archived depot manifest (v1 or v2)
/// </summary>
public record ArchivedManifest(
    string ManifestId,
    string GameId,
    int Version,
    string ManifestType,
    List<string> Languages,
    string ArchivePath,
    string CdnUrl,
    DateTime Timestamp,
    int FileCount,
    long TotalSize,
    HashSet<string> ChunksReferenced
);

/// <summary>
/// Represents an archived build manifest
/// </summary>
public record ArchivedBuild(
    string GameId,
    string BuildId,
    string BuildHash,
    string Platform,
    int Version,
    string ArchivePath,
    string CdnUrl,
    DateTime Timestamp,
    List<string> Dependencies,
    HashSet<string> ManifestsReferenced,
    string? RepositoryId = null,
    string VersionName = "",
    List<string>? Tags = null
)
{
    public List<string> Tags { get; init; } = Tags ?? new List<string>();
}

/// <summary>
/// Archive database structure for tracking builds
/// </summary>
public record ArchiveDatabase(
    List<ArchivedBuild> Builds,
    DateTime LastUpdated
);

/// <summary>
/// Result structure for archive operations
/// </summary>
public class ArchiveResult
{
    public string GameId { get; set; } = string.Empty;
    public string? BuildId { get; set; }
    public string? RepositoryId { get; set; }
    public int? RepositoryVersion { get; set; }
    public int ManifestsArchived { get; set; }
    public int ChunksArchived { get; set; }
    public int BlobsArchived { get; set; }
    public int DepotManifestsArchived { get; set; }
    public int DepotManifestsSkipped { get; set; }
    public List<string> Errors { get; set; } = new();
}

/// <summary>
/// Build listing result
/// </summary>
public class BuildListResult
{
    public string GameId { get; set; } = string.Empty;
    public List<BuildInfo> Builds { get; set; } = new();
    public string? Error { get; set; }
}

/// <summary>
/// Individual build information
/// </summary>
public class BuildInfo
{
    public string BuildId { get; set; } = string.Empty;
    public string Platform { get; set; } = string.Empty;
    public string Branch { get; set; } = "main";
    public bool Legacy { get; set; }
    public string? DatePublished { get; set; }
    public string Link { get; set; } = string.Empty;
    public int GenerationQueried { get; set; }
    public int? Version { get; set; }
    public string VersionName { get; set; } = string.Empty;
    public List<string> Tags { get; set; } = new();
    public string? LegacyBuildId { get; set; }
    public bool Public { get; set; } = true;
}

/// <summary>
/// Validation result structure
/// </summary>
public class ValidationResult
{
    public ValidationSummary ValidationSummary { get; set; } = new();
    public List<BuildValidationResult> BuildResults { get; set; } = new();
    public List<string> Errors { get; set; } = new();
}

/// <summary>
/// Validation summary statistics
/// </summary>
public class ValidationSummary
{
    public int TotalBuildsFound { get; set; }
    public int V1BuildsValidated { get; set; }
    public int V2BuildsValidated { get; set; }
    public int ValidationPassed { get; set; }
    public int ValidationFailed { get; set; }
    public int ChunksValidated { get; set; }
    public int ChunksFailed { get; set; }
    public int BlobsValidated { get; set; }
    public int BlobsFailed { get; set; }
}

/// <summary>
/// Individual build validation result
/// </summary>
public class BuildValidationResult
{
    public bool Success { get; set; }
    public string BuildKey { get; set; } = string.Empty;
    public int Version { get; set; }
    public int DepotManifestsFound { get; set; }
    public int ChunksValidated { get; set; }
    public int ChunksFailed { get; set; }
    public int BlobsValidated { get; set; }
    public int BlobsFailed { get; set; }
    public int FilesValidated { get; set; }
    public int FilesFailed { get; set; }
    public List<object> ValidationDetails { get; set; } = new();
    public List<string> Errors { get; set; } = new();
}