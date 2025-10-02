using Microsoft.Extensions.Logging;
using GalaxyDL.Models;
using GalaxyDL.Core;
using System.Text.Json;

namespace GalaxyDL.Services;

/// <summary>
/// Implementation of GOG Galaxy archiver service
/// </summary>
public class GogArchiverService : IGogArchiverService
{
    private readonly ILogger<GogArchiverService> _logger;
    private readonly IGogApiService _apiService;
    
    // Archive paths
    private string? _archiveRoot;
    private string? _buildsDir;
    private string? _manifestsDir;
    private string? _chunksDir;
    private string? _blobsDir;
    private string? _metadataDir;
    private string? _databasePath;
    
    // Archive state
    private readonly Dictionary<string, ArchivedBuild> _archivedBuilds = new();
    private readonly Dictionary<string, ArchivedChunk> _archivedChunks = new();
    private readonly Dictionary<string, ArchivedBlob> _archivedBlobs = new();
    private readonly Dictionary<string, ArchivedManifest> _archivedManifests = new();
    
    public GogArchiverService(ILogger<GogArchiverService> logger, IGogApiService apiService)
    {
        _logger = logger;
        _apiService = apiService;
    }

    public async Task InitializeAsync(string archiveRoot, string? authConfigPath = null, CancellationToken cancellationToken = default)
    {
        _archiveRoot = archiveRoot;
        _buildsDir = Path.Combine(_archiveRoot, GogConstants.ArchiveStructure.BUILDS_DIR);
        _manifestsDir = Path.Combine(_archiveRoot, GogConstants.ArchiveStructure.MANIFESTS_DIR);
        _chunksDir = Path.Combine(_archiveRoot, GogConstants.ArchiveStructure.CHUNKS_DIR);
        _blobsDir = Path.Combine(_archiveRoot, GogConstants.ArchiveStructure.BLOBS_DIR);
        _metadataDir = Path.Combine(_archiveRoot, GogConstants.ArchiveStructure.METADATA_DIR);
        _databasePath = Path.Combine(_metadataDir, GogConstants.ArchiveStructure.DATABASE_FILE);
        
        // Create directories
        GogUtils.EnsureDirectoryExists(_buildsDir);
        GogUtils.EnsureDirectoryExists(_manifestsDir);
        GogUtils.EnsureDirectoryExists(_chunksDir);
        GogUtils.EnsureDirectoryExists(_blobsDir);
        GogUtils.EnsureDirectoryExists(_metadataDir);
        
        // Create V1 and V2 specific directories
        GogUtils.EnsureDirectoryExists(Path.Combine(_buildsDir, GogConstants.ArchiveStructure.V1_MANIFESTS));
        GogUtils.EnsureDirectoryExists(Path.Combine(_buildsDir, GogConstants.ArchiveStructure.V2_META));
        GogUtils.EnsureDirectoryExists(Path.Combine(_manifestsDir, GogConstants.ArchiveStructure.V1_MANIFESTS));
        GogUtils.EnsureDirectoryExists(Path.Combine(_manifestsDir, GogConstants.ArchiveStructure.V2_DEPOTS));
        
        _logger.LogInformation("Initialized archiver with root: {ArchiveRoot}", _archiveRoot);
        
        // Initialize API service if auth config provided
        if (!string.IsNullOrEmpty(authConfigPath))
        {
            await _apiService.InitializeAsync(authConfigPath, cancellationToken);
        }
        
        // Load existing database
        await LoadDatabaseAsync(cancellationToken);
    }

    public async Task<ArchiveResult> ArchiveBuildAsync(string gameId, string buildId, List<string> platforms, List<string>? languages = null, int maxWorkers = 4, int? repositoryVersion = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new ArchiveResult
        {
            GameId = gameId,
            BuildId = buildId,
            RepositoryVersion = repositoryVersion
        };
        
        _logger.LogInformation("Archiving build {BuildId} for game {GameId}", buildId, gameId);
        
        try
        {
            // Repository mode if repositoryVersion is specified
            if (repositoryVersion.HasValue)
            {
                result.RepositoryId = buildId; // In repository mode, buildId is actually repositoryId
                return await ArchiveRepositoryAsync(gameId, buildId, repositoryVersion.Value, platforms, languages, maxWorkers, cancellationToken);
            }
            
            // Regular build mode
            platforms ??= new List<string> { GogConstants.Platforms.WINDOWS };
            languages ??= new List<string> { "en" };
            
            foreach (var platform in platforms)
            {
                // Check if already archived
                var buildKey = $"{gameId}_{buildId}_{platform}";
                if (_archivedBuilds.ContainsKey(buildKey))
                {
                    _logger.LogInformation("Build already archived: {BuildKey}", buildKey);
                    result.ManifestsArchived++;
                    continue;
                }
                
                // Try to find and archive the build manifest
                var archivedBuild = await ArchiveBuildManifestAsync(gameId, buildId, platform, cancellationToken);
                if (archivedBuild != null)
                {
                    result.ManifestsArchived++;
                    
                    // Now download depot manifests and their content
                    var depotResult = await ArchiveDepotManifestsAndContentAsync(gameId, archivedBuild, languages, maxWorkers, cancellationToken);
                    result.DepotManifestsArchived += depotResult.DepotManifestsArchived;
                    result.ChunksArchived += depotResult.ChunksArchived;
                    result.BlobsArchived += depotResult.BlobsArchived;
                    result.Errors.AddRange(depotResult.Errors);
                }
                else
                {
                    result.Errors.Add($"Failed to find build manifest for {gameId}/{buildId}/{platform}");
                }
            }
            
            // Save database after successful archiving
            if (result.ManifestsArchived > 0)
            {
                await SaveDatabaseAsync(cancellationToken);
            }
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving build {BuildId}", buildId);
            result.Errors.Add($"Failed to archive build {buildId}: {ex.Message}");
            return result;
        }
    }

    public async Task<ArchiveResult> ArchiveBuildAndDepotManifestsOnlyAsync(string gameId, string buildId, List<string>? platforms = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new ArchiveResult
        {
            GameId = gameId,
            BuildId = buildId
        };
        
        platforms ??= new List<string> { GogConstants.Platforms.WINDOWS };
        
        _logger.LogInformation("Archiving build and depot manifests only for {GameId}/{BuildId}", gameId, buildId);
        
        try
        {
            foreach (var platform in platforms)
            {
                // Archive build manifest
                var archivedBuild = await ArchiveBuildManifestAsync(gameId, buildId, platform, cancellationToken);
                if (archivedBuild != null)
                {
                    result.ManifestsArchived++;
                    
                    // Archive depot manifests only (no content)
                    var depotResult = await ArchiveDepotManifestsOnlyAsync(gameId, archivedBuild, cancellationToken);
                    result.DepotManifestsArchived += depotResult.DepotManifestsArchived;
                    result.Errors.AddRange(depotResult.Errors);
                }
                else
                {
                    result.Errors.Add($"Failed to find build manifest for {gameId}/{buildId}/{platform}");
                }
            }
            
            // Save database after successful archiving
            if (result.ManifestsArchived > 0)
            {
                await SaveDatabaseAsync(cancellationToken);
            }
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving manifests for {GameId}/{BuildId}", gameId, buildId);
            result.Errors.Add($"Failed to archive manifests: {ex.Message}");
            return result;
        }
    }

    public async Task<BuildListResult> ListBuildsAsync(string gameId, List<string>? platforms = null, int? generation = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new BuildListResult { GameId = gameId };
        
        // Smart platform defaults: Use common platforms first, but allow override for Linux
        platforms ??= GogConstants.Platforms.COMMON_PLATFORMS.ToList(); // Default to Windows + macOS
        
        _logger.LogInformation("Listing builds for game {GameId} on platforms: {Platforms}", gameId, string.Join(", ", platforms));
        
        // If user explicitly requested Linux, warn about rarity
        if (platforms.Contains(GogConstants.Platforms.LINUX))
        {
            _logger.LogInformation("?? Linux platform requested - Note: Linux GOG Galaxy manifests are very rare");
        }
        
        try
        {
            var allBuilds = new List<BuildInfo>();
            
            // Query both generations unless a specific one is requested
            var generationsToQuery = generation.HasValue ? new[] { generation.Value } : new[] { 1, 2 };
            
            foreach (var gen in generationsToQuery)
            {
                foreach (var platform in platforms)
                {
                    var platformSymbol = GogConstants.Platforms.PLATFORM_SYMBOLS.GetValueOrDefault(platform, "?");
                    var platformName = GogConstants.Platforms.PLATFORM_NAMES.GetValueOrDefault(platform, platform);
                    
                    _logger.LogDebug("Querying V{Generation} builds for {Symbol} {Platform}", gen, platformSymbol, platformName);
                    
                    var platformBuilds = await QueryBuildsForPlatformAsync(gameId, platform, gen, cancellationToken);
                    if (platformBuilds?.Any() == true)
                    {
                        allBuilds.AddRange(platformBuilds);
                        _logger.LogInformation("Found {Count} builds for {Symbol} {Platform} (V{Generation})", 
                                               platformBuilds.Count, platformSymbol, platformName, gen);
                    }
                    else
                    {
                        _logger.LogDebug("No V{Generation} builds found for {Symbol} {Platform}", gen, platformSymbol, platformName);
                    }
                }
            }
            
            // Remove duplicates and sort by date
            result.Builds = allBuilds
                .GroupBy(b => new { b.BuildId, b.Platform })
                .Select(g => g.OrderBy(b => b.GenerationQueried).First()) // Prefer V1 over V2 for same build
                .OrderByDescending(b => b.DatePublished)
                .ToList();
            
            _logger.LogInformation("Found {Count} unique builds for game {GameId}", result.Builds.Count, gameId);
            
            // Debug: Show platform distribution
            var platformDistribution = result.Builds
                .GroupBy(b => b.Platform)
                .ToDictionary(g => g.Key, g => g.Count());
            
            foreach (var (platform, count) in platformDistribution)
            {
                var symbol = GogConstants.Platforms.PLATFORM_SYMBOLS.GetValueOrDefault(platform, "?");
                var name = GogConstants.Platforms.PLATFORM_NAMES.GetValueOrDefault(platform, platform);
                _logger.LogInformation("Platform distribution: {Symbol} {Platform} = {Count} builds", symbol, name, count);
            }
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error listing builds for {GameId}", gameId);
            result.Error = $"Failed to list builds: {ex.Message}";
            return result;
        }
    }

    public async Task<Dictionary<string, object>> ListManifestsAsync(string gameId, string buildId, List<string>? platforms = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new Dictionary<string, object>
        {
            ["game_id"] = gameId,
            ["build_id"] = buildId,
            ["manifests"] = new List<object>()
        };
        
        platforms ??= new List<string> { GogConstants.Platforms.WINDOWS };
        
        _logger.LogInformation("Listing manifests for build {BuildId} of game {GameId}", buildId, gameId);
        
        try
        {
            // Try to get build manifest first
            var (buildManifest, _, version) = await GetBuildManifestDataAsync(gameId, buildId, platforms[0], cancellationToken);
            if (buildManifest == null)
            {
                result["error"] = $"Build manifest not found for {buildId}";
                return result;
            }
            
            var manifests = new List<object>();
            
            // Extract depot manifests from build manifest
            var depotManifestIds = ExtractDepotManifestIds(buildManifest, version);
            foreach (var manifestId in depotManifestIds)
            {
                manifests.Add(new
                {
                    manifest_id = manifestId,
                    version = version,
                    manifest_type = "depot"
                });
            }
            
            result["manifests"] = manifests;
            result["build_manifest_version"] = version;
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error listing manifests for {GameId}/{BuildId}", gameId, buildId);
            result["error"] = $"Failed to list manifests: {ex.Message}";
            return result;
        }
    }

    // Placeholder implementations for remaining interface methods...
    public async Task<List<ArchivedBuild>> ArchiveGameManifestsAsync(string gameId, List<string>? platforms = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        platforms ??= GogConstants.Platforms.ALL_PLATFORMS.ToList();
        
        _logger.LogInformation("Archiving game manifests for {GameId} on platforms: {Platforms}", gameId, string.Join(", ", platforms));
        
        var archived = new List<ArchivedBuild>();
        
        try
        {
            // TODO: Implement game manifest archiving logic
            _logger.LogWarning("Game manifest archiving not yet implemented");
            
            return archived;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving game manifests for {GameId}", gameId);
            return archived;
        }
    }

    public async Task<ArchiveResult> ArchiveRepositoryAndDepotManifestsOnlyAsync(string gameId, string repositoryId, int repositoryVersion, List<string>? platforms = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new ArchiveResult
        {
            GameId = gameId,
            RepositoryId = repositoryId,
            RepositoryVersion = repositoryVersion
        };
        
        platforms ??= new List<string> { GogConstants.Platforms.WINDOWS };
        
        _logger.LogInformation("Archiving repository and depot manifests only for {GameId}/{RepositoryId} (V{Version})", gameId, repositoryId, repositoryVersion);
        
        try
        {
            // TODO: Implement repository manifests-only archiving logic
            _logger.LogWarning("Repository and depot manifests-only archiving not yet implemented");
            result.Errors.Add("Repository manifests-only archiving functionality is not yet implemented");
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving repository manifests for {GameId}/{RepositoryId}", gameId, repositoryId);
            result.Errors.Add($"Failed to archive repository manifests: {ex.Message}");
            return result;
        }
    }

    public async Task<ValidationResult> ValidateArchiveComprehensiveAsync(string? gameId = null, string? buildId = null, List<string>? platforms = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new ValidationResult();
        platforms ??= new List<string> { GogConstants.Platforms.WINDOWS };
        
        _logger.LogInformation("Validating archive comprehensively for game {GameId}, build {BuildId}", gameId ?? "all", buildId ?? "all");
        
        try
        {
            // TODO: Implement comprehensive validation logic
            _logger.LogWarning("Comprehensive archive validation not yet implemented");
            result.Errors.Add("Comprehensive validation functionality is not yet implemented");
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during comprehensive validation");
            result.Errors.Add($"Validation failed: {ex.Message}");
            return result;
        }
    }

    public async Task<ArchiveResult> VerifyAndDownloadChunksForRepositoryAsync(string gameId, string repositoryId, int repositoryVersion, List<string>? platforms = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new ArchiveResult
        {
            GameId = gameId,
            RepositoryId = repositoryId,
            RepositoryVersion = repositoryVersion
        };
        
        platforms ??= new List<string> { GogConstants.Platforms.WINDOWS };
        
        _logger.LogInformation("Verifying and downloading chunks for repository {RepositoryId} (V{Version})", repositoryId, repositoryVersion);
        
        try
        {
            // TODO: Implement chunk verification and download logic
            _logger.LogWarning("Chunk verification and download not yet implemented");
            result.Errors.Add("Chunk verification and download functionality is not yet implemented");
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error verifying/downloading chunks for repository {RepositoryId}", repositoryId);
            result.Errors.Add($"Failed to verify/download chunks: {ex.Message}");
            return result;
        }
    }

    public async Task<ArchiveResult> ArchiveGameCompleteAsync(string gameId, List<string>? platforms = null, List<string>? languages = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new ArchiveResult { GameId = gameId };
        platforms ??= GogConstants.Platforms.ALL_PLATFORMS.ToList();
        languages ??= new List<string> { "en" };
        
        _logger.LogInformation("Archiving complete game {GameId}", gameId);
        
        try
        {
            // TODO: Implement complete game archiving logic
            _logger.LogWarning("Complete game archiving not yet implemented");
            result.Errors.Add("Complete game archiving functionality is not yet implemented");
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving complete game {GameId}", gameId);
            result.Errors.Add($"Failed to archive complete game: {ex.Message}");
            return result;
        }
    }

    public async Task<ArchiveResult> ArchiveManifestAsync(string gameId, string buildId, string manifestId, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var result = new ArchiveResult
        {
            GameId = gameId,
            BuildId = buildId
        };
        
        _logger.LogInformation("Archiving manifest {ManifestId} for build {BuildId}", manifestId, buildId);
        
        try
        {
            // TODO: Implement specific manifest archiving logic
            _logger.LogWarning("Specific manifest archiving not yet implemented");
            result.Errors.Add("Specific manifest archiving functionality is not yet implemented");
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving manifest {ManifestId}", manifestId);
            result.Errors.Add($"Failed to archive manifest: {ex.Message}");
            return result;
        }
    }

    public async Task<Dictionary<string, object>> GetArchiveStatsAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        var stats = new Dictionary<string, object>();
        
        try
        {
            // Calculate basic statistics
            var totalBuilds = _archivedBuilds.Count;
            var v1Builds = _archivedBuilds.Values.Count(b => b.Version == 1);
            var v2Builds = _archivedBuilds.Values.Count(b => b.Version == 2);
            var totalChunks = _archivedChunks.Count;
            var totalBlobs = _archivedBlobs.Count;
            
            // Calculate sizes
            var chunksSize = _archivedChunks.Values.Sum(c => c.CompressedSize);
            var blobsSize = _archivedBlobs.Values.Sum(b => b.TotalSize);
            var totalSize = chunksSize + blobsSize;
            
            // Count unique games
            var gamesArchived = _archivedBuilds.Values.Select(b => b.GameId).Distinct().Count();
            
            stats["total_builds"] = totalBuilds;
            stats["v1_builds"] = v1Builds;
            stats["v2_builds"] = v2Builds;
            stats["total_chunks"] = totalChunks;
            stats["total_blobs"] = totalBlobs;
            stats["chunks_size_bytes"] = chunksSize;
            stats["blobs_size_bytes"] = blobsSize;
            stats["total_size_bytes"] = totalSize;
            stats["total_size_gb"] = totalSize / (1024.0 * 1024.0 * 1024.0);
            stats["games_archived"] = gamesArchived;
            stats["archive_root"] = _archiveRoot;
            
            return stats;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error calculating archive statistics");
            stats["error"] = $"Failed to calculate statistics: {ex.Message}";
            return stats;
        }
    }

    public async Task SaveDatabaseAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        try
        {
            var database = new ArchiveDatabase(
                Builds: _archivedBuilds.Values.ToList(),
                LastUpdated: DateTime.UtcNow
            );
            
            await GogUtils.SafeWriteJsonAsync(_databasePath!, database, cancellationToken);
            _logger.LogDebug("Database saved with {Count} builds", _archivedBuilds.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error saving database");
            throw;
        }
    }

    public async Task LoadDatabaseAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        
        if (!File.Exists(_databasePath))
        {
            _logger.LogDebug("No existing database found");
            return;
        }
        
        try
        {
            var database = await GogUtils.ReadJsonAsync<ArchiveDatabase>(_databasePath!, cancellationToken);
            if (database?.Builds != null)
            {
                _archivedBuilds.Clear();
                foreach (var build in database.Builds)
                {
                    var key = $"{build.GameId}_{build.BuildId}_{build.Platform}";
                    _archivedBuilds[key] = build;
                }
                
                _logger.LogInformation("Loaded {Count} builds from database", _archivedBuilds.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading database");
        }
    }

    // Private helper methods for manifest archiving
    private async Task<ArchivedBuild?> ArchiveBuildManifestAsync(string gameId, string buildId, string platform, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogDebug("Archiving build manifest for {GameId}/{BuildId}/{Platform}", gameId, buildId, platform);
            
            // Try both V1 and V2 APIs to find the build
            var (buildManifestData, buildUrl, version) = await GetBuildManifestDataAsync(gameId, buildId, platform, cancellationToken);
            if (buildManifestData == null)
            {
                _logger.LogWarning("Build manifest not found for {GameId}/{BuildId}/{Platform}", gameId, buildId, platform);
                return null;
            }
            
            // Download raw manifest data
            var rawData = await _apiService.GetRawDataAsync(buildUrl, cancellationToken);
            if (rawData == null)
            {
                _logger.LogError("Failed to download raw manifest data from {Url}", buildUrl);
                return null;
            }
            
            // Save raw manifest to archive
            var rawPath = await SaveRawBuildManifestAsync(buildUrl, rawData, version, cancellationToken);
            
            // Create archived build entry
            var archivedBuild = new ArchivedBuild(
                GameId: gameId,
                BuildId: buildId,
                BuildHash: GogUtils.CalculateMd5(rawData),
                Platform: platform,
                Version: version,
                ArchivePath: GogUtils.GetRelativePath(_archiveRoot!, rawPath),
                CdnUrl: buildUrl,
                Timestamp: DateTime.UtcNow,
                Dependencies: ExtractDependencies(buildManifestData),
                ManifestsReferenced: ExtractManifestReferences(buildManifestData),
                RepositoryId: buildManifestData.GetValueOrDefault("legacy_build_id", buildId).ToString(),
                VersionName: buildManifestData.GetValueOrDefault("version_name", "").ToString() ?? "",
                Tags: ExtractTags(buildManifestData)
            );
            
            // Store in memory
            var buildKey = $"{gameId}_{buildId}_{platform}";
            _archivedBuilds[buildKey] = archivedBuild;
            
            _logger.LogInformation("Successfully archived build manifest: {GameId}/{BuildId}/{Platform} (V{Version})", gameId, buildId, platform, version);
            return archivedBuild;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving build manifest for {GameId}/{BuildId}/{Platform}", gameId, buildId, platform);
            return null;
        }
    }

    private async Task<(Dictionary<string, object>? data, string url, int version)> GetBuildManifestDataAsync(string gameId, string buildId, string platform, CancellationToken cancellationToken)
    {
        // First, try to get build info from the builds listing API to get the direct link
        try
        {
            _logger.LogDebug("Getting build info for {BuildId} to find direct manifest link", buildId);
            
            // Query the builds API to get the direct link for this build
            var buildsUrl = $"{GogConstants.GOG_CONTENT_SYSTEM}/products/{gameId}/os/{platform}/builds?generation=2";
            var (buildsData, _) = await _apiService.GetZlibEncodedAsync(buildsUrl, cancellationToken);
            
            if (buildsData?.TryGetValue("items", out var itemsObj) == true && itemsObj is JsonElement itemsElement)
            {
                foreach (var buildElement in itemsElement.EnumerateArray())
                {
                    var buildData = JsonSerializer.Deserialize<Dictionary<string, object>>(buildElement.GetRawText());
                    if (buildData?.GetValueOrDefault("build_id", "").ToString() == buildId)
                    {
                        // Found our build! Get the direct link
                        var directLink = buildData.GetValueOrDefault("link", "").ToString() ?? "";
                        var generation = buildData.TryGetValue("generation", out var genObj) && int.TryParse(genObj.ToString(), out var gen) ? gen : 2;
                        
                        if (!string.IsNullOrEmpty(directLink))
                        {
                            _logger.LogInformation("Found direct link for build {BuildId}: {Link} (Generation {Generation})", buildId, directLink, generation);
                            
                            // Download the manifest using the direct link
                            Dictionary<string, object>? manifestData;
                            if (generation == 2)
                            {
                                var (data, _) = await _apiService.GetZlibEncodedAsync(directLink, cancellationToken);
                                manifestData = data;
                            }
                            else
                            {
                                manifestData = await _apiService.GetJsonAsync(directLink, cancellationToken);
                            }
                            
                            if (manifestData != null)
                            {
                                return (manifestData, directLink, generation);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to get direct link for build {BuildId}, falling back to URL patterns", buildId);
        }
        
        // Fallback: Try the old URL pattern methods
        // Try V1 first (has precedence) - but we need the repository ID, not build ID
        try
        {
            // For V1, we need to get the repository ID first from the build listing or try common patterns
            // The V1 URL pattern is: /manifests/{gameId}/{platform}/{repositoryId}/repository.json
            // We'll need to discover the repository ID somehow
            
            // First, try if buildId is actually a repository ID (for V1 builds)
            var v1Url = $"{GogConstants.GOG_CDN}/content-system/v1/manifests/{gameId}/{platform}/{buildId}/repository.json";
            var v1Data = await _apiService.GetJsonAsync(v1Url, cancellationToken);
            if (v1Data != null)
            {
                _logger.LogDebug("Found V1 build manifest at {Url}", v1Url);
                return (v1Data, v1Url, 1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "V1 build manifest not found for {GameId}/{BuildId}/{Platform}", gameId, buildId, platform);
        }
        
        // Try V2 - but we need the repository ID in hex format
        try
        {
            // For V2, the buildId might be a repository ID in hex format
            // The V2 URL pattern is: /meta/{first2}/{next2}/{repositoryId}
            
            // If buildId looks like a hex repository ID (32 chars), use it directly
            if (buildId.Length == 32 && buildId.All(c => "0123456789abcdefABCDEF".Contains(c)))
            {
                var repositoryId = buildId.ToLowerInvariant();
                var v2Path = $"{repositoryId[..2]}/{repositoryId[2..4]}/{repositoryId}";
                var v2Url = $"{GogConstants.GOG_CDN}/content-system/v2/meta/{v2Path}";
                
                var (v2Data, _) = await _apiService.GetZlibEncodedAsync(v2Url, cancellationToken);
                if (v2Data != null)
                {
                    _logger.LogDebug("Found V2 build manifest at {Url}", v2Url);
                    return (v2Data, v2Url, 2);
                }
            }
            
            // Fallback: Try the original V2 API pattern (what we were using before)
            var fallbackV2Url = $"{GogConstants.GOG_CONTENT_SYSTEM}/products/{gameId}/os/{platform}/builds/{buildId}";
            var (fallbackData, _) = await _apiService.GetZlibEncodedAsync(fallbackV2Url, cancellationToken);
            if (fallbackData != null)
            {
                _logger.LogDebug("Found V2 build manifest at {Url}", fallbackV2Url);
                return (fallbackData, fallbackV2Url, 2);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "V2 build manifest not found for {GameId}/{BuildId}/{Platform}", gameId, buildId, platform);
        }
        
        return (null, "", 0);
    }

    private async Task<string> SaveRawBuildManifestAsync(string cdnUrl, byte[] rawData, int version, CancellationToken cancellationToken)
    {
        string savePath;
        
        if (version == 1)
        {
            // V1: Save in builds/v1/manifests/{game_id}/{platform}/{build_id}/repository.json
            var urlParts = cdnUrl.Split('/');
            var gameId = urlParts[^3];
            var platform = urlParts[^2];
            var buildId = urlParts[^1];
            
            var dirPath = Path.Combine(_buildsDir!, "v1", "manifests", gameId, platform, buildId);
            GogUtils.EnsureDirectoryExists(dirPath);
            savePath = Path.Combine(dirPath, "repository.json");
        }
        else
        {
            // V2: Extract path from URL and save in builds/v2/meta/{path_from_url}
            var urlParts = cdnUrl.Split("/builds/");
            if (urlParts.Length > 1)
            {
                var buildPath = urlParts[1];
                // Convert to path structure: abc123def456 -> ab/c1/abc123def456
                if (buildPath.Length >= 4)
                {
                    var structuredPath = $"{buildPath[..2]}/{buildPath[2..4]}/{buildPath}";
                    var dirPath = Path.Combine(_buildsDir!, "v2", "meta", Path.GetDirectoryName(structuredPath) ?? "");
                    GogUtils.EnsureDirectoryExists(dirPath);
                    savePath = Path.Combine(_buildsDir!, "v2", "meta", structuredPath);
                }
                else
                {
                    var dirPath = Path.Combine(_buildsDir!, "v2", "meta");
                    GogUtils.EnsureDirectoryExists(dirPath);
                    savePath = Path.Combine(dirPath, buildPath);
                }
            }
            else
            {
                // Fallback
                var fileName = cdnUrl.Split('/').Last();
                savePath = Path.Combine(_buildsDir!, "v2", "meta", fileName);
            }
        }
        
        // Save raw data
        await GogUtils.SafeWriteAllBytesAsync(savePath, rawData, cancellationToken);
        
        // Also save prettified JSON if possible
        try
        {
            var prettifiedPath = savePath + ".json";
            var (decompressed, wasCompressed) = GogUtils.TryDecompress(rawData);
            var jsonString = System.Text.Encoding.UTF8.GetString(decompressed);
            var jsonData = JsonSerializer.Deserialize<object>(jsonString);
            var prettifiedJson = JsonSerializer.Serialize(jsonData, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(prettifiedPath, prettifiedJson, cancellationToken);
            
            _logger.LogDebug("Saved prettified manifest: {Path}", prettifiedPath);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to create prettified manifest for {Path}", savePath);
        }
        
        _logger.LogDebug("Saved raw build manifest: {Path}", savePath);
        return savePath;
    }

    private async Task<ArchiveResult> ArchiveDepotManifestsOnlyAsync(string gameId, ArchivedBuild build, CancellationToken cancellationToken)
    {
        var result = new ArchiveResult { GameId = gameId, BuildId = build.BuildId };
        
        try
        {
            // Load build manifest to extract depot references
            var buildManifestPath = Path.Combine(_archiveRoot!, build.ArchivePath);
            var buildManifestData = await LoadBuildManifestFromFileAsync(buildManifestPath, build.Version, cancellationToken);
            if (buildManifestData == null)
            {
                result.Errors.Add($"Failed to load build manifest from {buildManifestPath}");
                return result;
            }
            
            // Extract depot manifest IDs
            var depotManifestIds = ExtractDepotManifestIds(buildManifestData, build.Version);
            _logger.LogInformation("Found {Count} depot manifests to archive", depotManifestIds.Count);
            
            // Download each depot manifest
            foreach (var depotManifestId in depotManifestIds)
            {
                var depotResult = await ArchiveDepotManifestAsync(gameId, depotManifestId, build.Version, build.Platform, build.BuildId, cancellationToken);
                if (depotResult)
                {
                    result.DepotManifestsArchived++;
                }
                else
                {
                    result.Errors.Add($"Failed to archive depot manifest {depotManifestId}");
                }
            }
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving depot manifests for build {BuildId}", build.BuildId);
            result.Errors.Add($"Failed to archive depot manifests: {ex.Message}");
            return result;
        }
    }

    private async Task<ArchiveResult> ArchiveDepotManifestsAndContentAsync(string gameId, ArchivedBuild build, List<string> languages, int maxWorkers, CancellationToken cancellationToken)
    {
        var result = new ArchiveResult { GameId = gameId, BuildId = build.BuildId };
        
        try
        {
            // First, archive depot manifests
            var manifestsResult = await ArchiveDepotManifestsOnlyAsync(gameId, build, cancellationToken);
            result.DepotManifestsArchived = manifestsResult.DepotManifestsArchived;
            result.Errors.AddRange(manifestsResult.Errors);
            
            // TODO: Implement content downloading (chunks/blobs)
            _logger.LogInformation("Content downloading not yet implemented for build {BuildId}", build.BuildId);
            result.Errors.Add("Content downloading functionality is not yet implemented");
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving depot manifests and content for build {BuildId}", build.BuildId);
            result.Errors.Add($"Failed to archive depot manifests and content: {ex.Message}");
            return result;
        }
    }

    private async Task<bool> ArchiveDepotManifestAsync(string gameId, string manifestId, int version, string platform, string buildId, CancellationToken cancellationToken)
    {
        try
        {
            string depotUrl;
            string savePath;
            
            if (version == 1)
            {
                // V1 depot manifest URL
                depotUrl = $"{GogConstants.GOG_CDN}/content-system/v1/manifests/{gameId}/{platform}/{buildId}/{manifestId}";
                var dirPath = Path.Combine(_manifestsDir!, "v1", "manifests", gameId, platform, buildId);
                GogUtils.EnsureDirectoryExists(dirPath);
                savePath = Path.Combine(dirPath, manifestId);
            }
            else
            {
                // V2 depot manifest URL
                var galaxyPath = GogUtils.GalaxyPath(manifestId);
                depotUrl = $"{GogConstants.GOG_MANIFESTS_COLLECTOR}/manifests/depots/{galaxyPath}";
                var dirPath = Path.Combine(_manifestsDir!, "v2", "depots", Path.GetDirectoryName(galaxyPath) ?? "");
                GogUtils.EnsureDirectoryExists(dirPath);
                savePath = Path.Combine(_manifestsDir!, "v2", "depots", galaxyPath);
            }
            
            // Check if already exists
            if (File.Exists(savePath))
            {
                _logger.LogDebug("Depot manifest already exists: {ManifestId}", manifestId);
                return true;
            }
            
            // Download depot manifest
            var rawData = await _apiService.GetRawDataAsync(depotUrl, cancellationToken);
            if (rawData == null)
            {
                _logger.LogWarning("Failed to download depot manifest {ManifestId} from {Url}", manifestId, depotUrl);
                return false;
            }
            
            // Save raw depot manifest
            await GogUtils.SafeWriteAllBytesAsync(savePath, rawData, cancellationToken);
            
            // Save prettified version if possible
            try
            {
                var prettifiedPath = savePath + ".json";
                var (decompressed, wasCompressed) = GogUtils.TryDecompress(rawData);
                var jsonString = System.Text.Encoding.UTF8.GetString(decompressed);
                var jsonData = JsonSerializer.Deserialize<object>(jsonString);
                var prettifiedJson = JsonSerializer.Serialize(jsonData, new JsonSerializerOptions { WriteIndented = true });
                await File.WriteAllTextAsync(prettifiedPath, prettifiedJson, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to create prettified depot manifest for {ManifestId}", manifestId);
            }
            
            _logger.LogDebug("Successfully archived depot manifest: {ManifestId}", manifestId);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving depot manifest {ManifestId}", manifestId);
            return false;
        }
    }

    private async Task<Dictionary<string, object>?> LoadBuildManifestFromFileAsync(string filePath, int version, CancellationToken cancellationToken)
    {
        try
        {
            var rawData = await File.ReadAllBytesAsync(filePath, cancellationToken);
            var (decompressed, _) = GogUtils.TryDecompress(rawData);
            var jsonString = System.Text.Encoding.UTF8.GetString(decompressed);
            return JsonSerializer.Deserialize<Dictionary<string, object>>(jsonString, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                PropertyNameCaseInsensitive = true
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load build manifest from {FilePath}", filePath);
            return null;
        }
    }

    private async Task<List<BuildInfo>?> QueryBuildsForPlatformAsync(string gameId, string platform, int generation, CancellationToken cancellationToken)
    {
        try
        {
            var allBuilds = new List<BuildInfo>();
            
            if (generation == 1)
            {
                // V1 builds: Try the old V1 API first (usually 404)
                try
                {
                    var v1Url = $"{GogConstants.GOG_CDN}/content-system/v1/meta/{gameId}/{platform}";
                    _logger.LogDebug("Trying V1 legacy API: {Url}", v1Url);
                    var v1Data = await _apiService.GetJsonAsync(v1Url, cancellationToken);
                    if (v1Data != null && v1Data.TryGetValue("builds", out var v1BuildsObj) && v1BuildsObj is JsonElement v1BuildsElement)
                    {
                        foreach (var buildElement in v1BuildsElement.EnumerateArray())
                        {
                            var build = ParseV1Build(buildElement, platform, generation);
                            if (build != null)
                            {
                                allBuilds.Add(build);
                            }
                        }
                        _logger.LogInformation("Found {Count} builds from V1 legacy API", allBuilds.Count);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "V1 legacy API failed (expected for most games)");
                }
                
                // V1 builds via V2 API: Query generation=1 on the V2 endpoint!
                var v1ViaV2Url = $"{GogConstants.GOG_CONTENT_SYSTEM}/products/{gameId}/os/{platform}/builds?generation=1";
                _logger.LogDebug("Querying V1 builds via V2 API: {Url}", v1ViaV2Url);
                
                var (v1ViaV2Data, _) = await _apiService.GetZlibEncodedAsync(v1ViaV2Url, cancellationToken);
                if (v1ViaV2Data?.TryGetValue("items", out var v1ItemsObj) == true && v1ItemsObj is JsonElement v1ItemsElement)
                {
                    foreach (var buildElement in v1ItemsElement.EnumerateArray())
                    {
                        var build = ParseV2UnifiedBuild(buildElement, platform, cancellationToken);
                        if (build != null)
                        {
                            allBuilds.Add(build);
                            _logger.LogDebug("Found V1 build via V2 API: {BuildId} (Generation: {Generation})", build.BuildId, build.GenerationQueried);
                        }
                    }
                    
                    if (v1ViaV2Data.TryGetValue("total_count", out var totalCountObj))
                    {
                        _logger.LogInformation("V1 builds via V2 API: Found {Found} out of {Total} reported", v1ItemsElement.GetArrayLength(), totalCountObj);
                    }
                }
            }
            else // generation == 2
            {
                // V2 builds: Query generation=2 on the V2 endpoint
                var offset = 0;
                const int limit = 50;
                bool hasMore = true;
                
                while (hasMore)
                {
                    var v2Url = $"{GogConstants.GOG_CONTENT_SYSTEM}/products/{gameId}/os/{platform}/builds?generation=2&limit={limit}&offset={offset}";
                    _logger.LogDebug("Querying V2 builds: {Url}", v2Url);
                    
                    var (v2Data, _) = await _apiService.GetZlibEncodedAsync(v2Url, cancellationToken);
                    if (v2Data == null)
                    {
                        _logger.LogDebug("No V2 builds data returned at offset {Offset}", offset);
                        break;
                    }
                    
                    var builds = new List<BuildInfo>();
                    
                    if (v2Data.TryGetValue("items", out var v2ItemsObj) && v2ItemsObj is JsonElement v2ItemsElement)
                    {
                        foreach (var buildElement in v2ItemsElement.EnumerateArray())
                        {
                            var build = ParseV2UnifiedBuild(buildElement, platform, cancellationToken);
                            if (build != null)
                            {
                                builds.Add(build);
                                _logger.LogDebug("Found V2 build: {BuildId} (Generation: {Generation})", build.BuildId, build.GenerationQueried);
                            }
                        }
                        
                        // Check pagination
                        if (v2Data.TryGetValue("has_more", out var hasMoreValue) && hasMoreValue is JsonElement hasMoreElement)
                        {
                            hasMore = hasMoreElement.ValueKind == JsonValueKind.True;
                        }
                        else
                        {
                            hasMore = false;
                        }
                        
                        // Log counts
                        if (v2Data.TryGetValue("total_count", out var totalCountObj))
                        {
                            _logger.LogDebug("V2 Page {Offset}: Got {PageCount} builds, total reported: {TotalCount}", offset, builds.Count, totalCountObj);
                            
                            // Special check for the macOS issue - if total_count is much higher than what we're getting
                            if (int.TryParse(totalCountObj.ToString(), out var totalCount) && totalCount > 10 && builds.Count < 10)
                            {
                                _logger.LogWarning("V2 API reports {Total} builds but we only got {Count} - there may be private builds or pagination issues", totalCount, builds.Count);
                            }
                        }
                    }
                    
                    allBuilds.AddRange(builds);
                    
                    // Stop if no builds found in this page
                    if (builds.Count == 0)
                    {
                        hasMore = false;
                    }
                    
                    offset += limit;
                    
                    // Safety check
                    if (offset > 1000)
                    {
                        _logger.LogWarning("Stopping V2 pagination at offset {Offset}", offset);
                        break;
                    }
                }
            }
            
            _logger.LogInformation("Total builds found for V{Generation} {Platform}: {Count}", generation, platform, allBuilds.Count);
            return allBuilds.Any() ? allBuilds : null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error querying V{Generation} builds for platform {Platform}", generation, platform);
            return null;
        }
    }

    // New unified parser for the V2 API that returns both V1 and V2 builds
    private static BuildInfo? ParseV2UnifiedBuild(JsonElement buildElement, string platform, CancellationToken cancellationToken)
    {
        try
        {
            var buildData = JsonSerializer.Deserialize<Dictionary<string, object>>(buildElement.GetRawText());
            if (buildData == null) return null;
            
            // Debug: Log what fields we have
            var availableFields = string.Join(", ", buildData.Keys);
            Console.WriteLine($"Unified Build fields: {availableFields}");
            
            var buildId = buildData.GetValueOrDefault("build_id", "unknown").ToString() ?? "unknown";
            var datePublished = buildData.GetValueOrDefault("date_published", "").ToString();
            
            // The API now tells us directly what generation this build is!
            var generation = 2; // Default to V2
            var isLegacy = false;
            
            if (buildData.TryGetValue("generation", out var generationObj) && int.TryParse(generationObj.ToString(), out var apiGeneration))
            {
                generation = apiGeneration;
                isLegacy = (apiGeneration == 1); // V1 builds are legacy
            }
            
            // Extract the direct link provided by GOG API
            var link = buildData.GetValueOrDefault("link", "").ToString() ?? "";
            
            // Extract legacy_build_id if present (for V1 builds)
            var legacyBuildId = buildData.GetValueOrDefault("legacy_build_id", "").ToString();
            
            Console.WriteLine($"Build {buildId}: Generation={generation}, Legacy={isLegacy}, Link={link}");
            
            return new BuildInfo
            {
                BuildId = buildId,
                Platform = platform,
                Branch = buildData.GetValueOrDefault("branch", "main").ToString() ?? "main",
                Legacy = isLegacy,
                DatePublished = datePublished,
                Link = link, // This is the direct manifest URL from GOG!
                GenerationQueried = generation, // Use the actual generation from API
                Version = buildData.TryGetValue("version", out var versionObj) && int.TryParse(versionObj.ToString(), out var version) ? version : null,
                VersionName = buildData.GetValueOrDefault("version_name", "").ToString() ?? "",
                Tags = buildData.TryGetValue("tags", out var tagsObj) && tagsObj is JsonElement tagsElement 
                    ? tagsElement.EnumerateArray().Select(t => t.GetString() ?? "").ToList() 
                    : new List<string>(),
                LegacyBuildId = legacyBuildId,
                Public = buildData.TryGetValue("public", out var publicObj) && bool.TryParse(publicObj.ToString(), out var isPublic) && isPublic
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parsing unified build: {ex.Message}");
            return null;
        }
    }

    private void EnsureInitialized()
    {
        if (string.IsNullOrEmpty(_archiveRoot))
        {
            throw new InvalidOperationException("Archiver not initialized. Call InitializeAsync first.");
        }
    }

    // Helper methods
    private async Task<ArchiveResult> ArchiveRepositoryAsync(string gameId, string repositoryId, int repositoryVersion, List<string> platforms, List<string>? languages, int maxWorkers, CancellationToken cancellationToken)
    {
        var result = new ArchiveResult
        {
            GameId = gameId,
            RepositoryId = repositoryId,
            RepositoryVersion = repositoryVersion
        };
        
        _logger.LogInformation("Repository mode: downloading repository {RepositoryId} using V{Version} API", repositoryId, repositoryVersion);
        
        try
        {
            // TODO: Implement repository archiving logic
            _logger.LogWarning("Repository archiving not yet implemented");
            result.Errors.Add("Repository archiving functionality is not yet implemented");
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error archiving repository {RepositoryId}", repositoryId);
            result.Errors.Add($"Failed to archive repository: {ex.Message}");
            return result;
        }
    }

    // Static helper methods
    private static List<string> ExtractDepotManifestIds(Dictionary<string, object> buildManifest, int version)
    {
        var manifestIds = new List<string>();
        
        try
        {
            if (version == 1)
            {
                // V1: Look in product.depots
                if (buildManifest.TryGetValue("product", out var productObj) && productObj is JsonElement productElement)
                {
                    var product = JsonSerializer.Deserialize<Dictionary<string, object>>(productElement.GetRawText());
                    if (product?.TryGetValue("depots", out var depotsObj) == true && depotsObj is JsonElement depotsElement)
                    {
                        foreach (var depotElement in depotsElement.EnumerateArray())
                        {
                            var depot = JsonSerializer.Deserialize<Dictionary<string, object>>(depotElement.GetRawText());
                            if (depot?.TryGetValue("manifest", out var manifestObj) == true && manifestObj != null)
                            {
                                manifestIds.Add(manifestObj.ToString() ?? "");
                            }
                        }
                    }
                }
            }
            else
            {
                // V2: Look in depots array
                if (buildManifest.TryGetValue("depots", out var depotsObj) && depotsObj is JsonElement depotsElement)
                {
                    foreach (var depotElement in depotsElement.EnumerateArray())
                    {
                        var depot = JsonSerializer.Deserialize<Dictionary<string, object>>(depotElement.GetRawText());
                        if (depot?.TryGetValue("manifest", out var manifestObj) == true && manifestObj != null)
                        {
                            manifestIds.Add(manifestObj.ToString() ?? "");
                        }
                    }
                }
            }
        }
        catch (Exception)
        {
            // Ignore parsing errors, return what we found
        }
        
        return manifestIds.Where(id => !string.IsNullOrEmpty(id)).ToList();
    }

    private static List<string> ExtractDependencies(Dictionary<string, object> buildManifest)
    {
        var dependencies = new List<string>();
        
        try
        {
            if (buildManifest.TryGetValue("dependencies", out var depsObj) && depsObj is JsonElement depsElement)
            {
                foreach (var depElement in depsElement.EnumerateArray())
                {
                    if (depElement.ValueKind == JsonValueKind.String)
                    {
                        var dep = depElement.GetString();
                        if (!string.IsNullOrEmpty(dep))
                        {
                            dependencies.Add(dep);
                        }
                    }
                }
            }
        }
        catch (Exception)
        {
            // Ignore parsing errors
        }
        
        return dependencies;
    }

    private static HashSet<string> ExtractManifestReferences(Dictionary<string, object> buildManifest)
    {
        var references = new HashSet<string>();
        
        // This would be populated by depot manifest IDs
        var depotIds = ExtractDepotManifestIds(buildManifest, 1); // Try V1 format first
        if (!depotIds.Any())
        {
            depotIds = ExtractDepotManifestIds(buildManifest, 2); // Try V2 format
        }
        
        foreach (var id in depotIds)
        {
            references.Add(id);
        }
        
        return references;
    }

    private static List<string> ExtractTags(Dictionary<string, object> buildManifest)
    {
        var tags = new List<string>();
        
        try
        {
            if (buildManifest.TryGetValue("tags", out var tagsObj) && tagsObj is JsonElement tagsElement)
            {
                foreach (var tagElement in tagsElement.EnumerateArray())
                {
                    var tag = tagElement.GetString();
                    if (!string.IsNullOrEmpty(tag))
                    {
                        tags.Add(tag);
                    }
                }
            }
        }
        catch (Exception)
        {
            // Ignore parsing errors
        }
        
        return tags;
    }

    private static BuildInfo? ParseV1Build(JsonElement buildElement, string platform, int generation)
    {
        try
        {
            var buildData = JsonSerializer.Deserialize<Dictionary<string, object>>(buildElement.GetRawText());
            if (buildData == null) return null;
            
            return new BuildInfo
            {
                BuildId = buildData.GetValueOrDefault("build_id", "unknown").ToString() ?? "unknown",
                Platform = platform,
                Branch = buildData.GetValueOrDefault("branch", "main").ToString() ?? "main",
                Legacy = true,
                DatePublished = buildData.GetValueOrDefault("date_published", "").ToString(),
                Link = buildData.GetValueOrDefault("link", "").ToString() ?? "",
                GenerationQueried = generation,
                Version = buildData.TryGetValue("version", out var versionObj) && int.TryParse(versionObj.ToString(), out var version) ? version : null,
                VersionName = buildData.GetValueOrDefault("version_name", "").ToString() ?? "",
                Tags = buildData.TryGetValue("tags", out var tagsObj) && tagsObj is JsonElement tagsElement 
                    ? tagsElement.EnumerateArray().Select(t => t.GetString() ?? "").ToList() 
                    : new List<string>(),
                LegacyBuildId = buildData.GetValueOrDefault("legacy_build_id", "").ToString(),
                Public = buildData.TryGetValue("public", out var publicObj) && bool.TryParse(publicObj.ToString(), out var isPublic) && isPublic
            };
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static BuildInfo? ParseV2Build(JsonElement buildElement, string platform, int generation)
    {
        try
        {
            var buildData = JsonSerializer.Deserialize<Dictionary<string, object>>(buildElement.GetRawText());
            if (buildData == null) return null;
            
            // Debug: Log what fields we have
            var availableFields = string.Join(", ", buildData.Keys);
            Console.WriteLine($"V2 Build fields: {availableFields}");
            
            var buildId = buildData.GetValueOrDefault("build_id", "unknown").ToString() ?? "unknown";
            var datePublished = buildData.GetValueOrDefault("date_published", "").ToString();
            
            // Determine if this is a legacy build based on date or other indicators
            var isLegacy = false;
            if (!string.IsNullOrEmpty(datePublished) && DateTime.TryParse(datePublished, out var publishDate))
            {
                // Consider builds from before 2020 as legacy (V1 era)
                isLegacy = publishDate.Year < 2020;
            }
            
            // Also check for legacy_build_id field or other V1 indicators
            if (buildData.ContainsKey("legacy_build_id") || buildData.ContainsKey("legacy"))
            {
                isLegacy = true;
            }
            
            return new BuildInfo
            {
                BuildId = buildId,
                Platform = platform,
                Branch = buildData.GetValueOrDefault("branch", "main").ToString() ?? "main",
                Legacy = isLegacy, // Fixed: Determine legacy status properly
                DatePublished = datePublished,
                Link = buildData.GetValueOrDefault("link", "").ToString() ?? "",
                GenerationQueried = generation,
                Version = buildData.TryGetValue("version", out var versionObj) && int.TryParse(versionObj.ToString(), out var version) ? version : null,
                VersionName = buildData.GetValueOrDefault("version_name", "").ToString() ?? "",
                Tags = buildData.TryGetValue("tags", out var tagsObj) && tagsObj is JsonElement tagsElement 
                    ? tagsElement.EnumerateArray().Select(t => t.GetString() ?? "").ToList() 
                    : new List<string>(),
                LegacyBuildId = buildData.GetValueOrDefault("legacy_build_id", "").ToString(),
                Public = buildData.TryGetValue("public", out var publicObj) && bool.TryParse(publicObj.ToString(), out var isPublic) && isPublic
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parsing V2 build: {ex.Message}");
            return null;
        }
    }
}