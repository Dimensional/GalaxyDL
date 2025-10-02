using GalaxyDL.Models;

namespace GalaxyDL.Services;

/// <summary>
/// Interface for GOG Galaxy archiver functionality
/// </summary>
public interface IGogArchiverService
{
    /// <summary>
    /// Archive a specific build
    /// </summary>
    Task<ArchiveResult> ArchiveBuildAsync(string gameId, string buildId, List<string> platforms, List<string>? languages = null, int maxWorkers = 4, int? repositoryVersion = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Archive all manifests for a game
    /// </summary>
    Task<List<ArchivedBuild>> ArchiveGameManifestsAsync(string gameId, List<string>? platforms = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Archive build manifests and all their referenced depot manifests - no chunks/blobs
    /// </summary>
    Task<ArchiveResult> ArchiveBuildAndDepotManifestsOnlyAsync(string gameId, string buildId, List<string>? platforms = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Archive repository build manifests and all their referenced depot manifests - no chunks/blobs
    /// </summary>
    Task<ArchiveResult> ArchiveRepositoryAndDepotManifestsOnlyAsync(string gameId, string repositoryId, int repositoryVersion, List<string>? platforms = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Validate archive integrity comprehensively
    /// </summary>
    Task<ValidationResult> ValidateArchiveComprehensiveAsync(string? gameId = null, string? buildId = null, List<string>? platforms = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// List available builds for a game
    /// </summary>
    Task<BuildListResult> ListBuildsAsync(string gameId, List<string>? platforms = null, int? generation = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// List manifests within a specific build
    /// </summary>
    Task<Dictionary<string, object>> ListManifestsAsync(string gameId, string buildId, List<string>? platforms = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Verify and download chunks for a repository
    /// </summary>
    Task<ArchiveResult> VerifyAndDownloadChunksForRepositoryAsync(string gameId, string repositoryId, int repositoryVersion, List<string>? platforms = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Archive a complete game (manifests and all content)
    /// </summary>
    Task<ArchiveResult> ArchiveGameCompleteAsync(string gameId, List<string>? platforms = null, List<string>? languages = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Archive a specific manifest
    /// </summary>
    Task<ArchiveResult> ArchiveManifestAsync(string gameId, string buildId, string manifestId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get archive statistics
    /// </summary>
    Task<Dictionary<string, object>> GetArchiveStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Save archive database
    /// </summary>
    Task SaveDatabaseAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Load archive database
    /// </summary>
    Task LoadDatabaseAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Initialize the archiver with archive root path
    /// </summary>
    Task InitializeAsync(string archiveRoot, string? authConfigPath = null, CancellationToken cancellationToken = default);
}