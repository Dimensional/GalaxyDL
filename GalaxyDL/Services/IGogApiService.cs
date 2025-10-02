using GalaxyDL.Models;

namespace GalaxyDL.Services;

/// <summary>
/// Interface for GOG API communication
/// </summary>
public interface IGogApiService
{
    /// <summary>
    /// Initialize the API service with authentication
    /// </summary>
    Task InitializeAsync(string authConfigPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Authenticate using authorization code from OAuth2 flow
    /// </summary>
    Task<bool> AuthenticateWithCodeAsync(string authorizationCode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get item data from products endpoint
    /// </summary>
    Task<Dictionary<string, object>?> GetItemDataAsync(string id, List<string>? expanded = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get game details
    /// </summary>
    Task<Dictionary<string, object>?> GetGameDetailsAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Check if user owns a game
    /// </summary>
    Task<bool> DoesUserOwnAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get secure links for content download
    /// </summary>
    Task<List<Dictionary<string, object>>?> GetSecureLinksAsync(string gameId, string path = "/", int generation = 2, CancellationToken cancellationToken = default);

    /// <summary>
    /// Download JSON data from URL
    /// </summary>
    Task<Dictionary<string, object>?> GetJsonAsync(string url, CancellationToken cancellationToken = default);

    /// <summary>
    /// Download raw data from URL
    /// </summary>
    Task<byte[]?> GetRawDataAsync(string url, CancellationToken cancellationToken = default);

    /// <summary>
    /// Download compressed data and decompress it
    /// </summary>
    Task<(Dictionary<string, object>? data, Dictionary<string, string> headers)> GetZlibEncodedAsync(string url, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get HTTP client for custom requests
    /// </summary>
    HttpClient GetHttpClient();

    /// <summary>
    /// Check if credentials are expired
    /// </summary>
    bool IsCredentialExpired();

    /// <summary>
    /// Refresh credentials
    /// </summary>
    Task<bool> RefreshCredentialsAsync(CancellationToken cancellationToken = default);
}