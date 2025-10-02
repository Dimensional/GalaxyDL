using Microsoft.Extensions.Logging;
using GalaxyDL.Core;
using System.Text.Json;
using System.Net;
using System.Text;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace GalaxyDL.Services;

/// <summary>
/// Service for handling GOG OAuth2 authentication with embedded browser
/// </summary>
public class GogAuthService : IDisposable
{
    private readonly ILogger<GogAuthService> _logger;
    private readonly IGogApiService _apiService;
    private HttpListener? _httpListener;
    private bool _disposed = false;

    public GogAuthService(ILogger<GogAuthService> logger, IGogApiService apiService)
    {
        _logger = logger;
        _apiService = apiService;
    }

    /// <summary>
    /// Perform OAuth2 authentication flow and save credentials
    /// </summary>
    public async Task<bool> AuthenticateAsync(string authConfigPath, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Starting GOG OAuth2 authentication flow");
            
            // Generate state parameter for security
            var state = Guid.NewGuid().ToString("N");
            var redirectUri = "https://embed.gog.com/on_login_success?origin=client";
            
            // Build OAuth2 authorization URL (following the exact format from the testing guide)
            var authUrl = BuildAuthorizationUrl(redirectUri, state);
            
            Console.WriteLine("?? Starting GOG authentication...");
            Console.WriteLine($"?? Opening browser to: {authUrl}");
            Console.WriteLine();
            Console.WriteLine("Please log in to your GOG account in the browser window.");
            Console.WriteLine();
            Console.WriteLine("??  IMPORTANT: After logging in, you'll see a success page.");
            Console.WriteLine("Copy the FULL URL from your browser's address bar and paste it here.");
            Console.WriteLine("The URL will look like: https://embed.gog.com/on_login_success?origin=client&code=...");
            Console.WriteLine();
            
            // Open browser
            OpenBrowser(authUrl);
            
            // Wait for user to paste the callback URL
            Console.Write("Paste the callback URL here: ");
            var callbackUrl = Console.ReadLine();
            
            if (string.IsNullOrEmpty(callbackUrl))
            {
                Console.WriteLine("? No URL provided.");
                return false;
            }
            
            // Extract authorization code from the URL
            var authCode = ExtractAuthorizationCode(callbackUrl, state);
            
            if (string.IsNullOrEmpty(authCode))
            {
                Console.WriteLine("? Could not extract authorization code from URL.");
                return false;
            }
            
            Console.WriteLine("? Authorization code extracted!");
            Console.WriteLine("?? Exchanging code for access token...");
            
            // Exchange authorization code for tokens
            var success = await _apiService.AuthenticateWithCodeAsync(authCode, cancellationToken);
            
            if (success)
            {
                Console.WriteLine("? Authentication successful!");
                Console.WriteLine($"?? Credentials saved to: {Path.GetFullPath(authConfigPath)}");
                return true;
            }
            else
            {
                Console.WriteLine("? Failed to exchange authorization code for tokens.");
                return false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during authentication flow");
            Console.WriteLine($"? Authentication error: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Build the OAuth2 authorization URL (following exact format from testing guide)
    /// </summary>
    private static string BuildAuthorizationUrl(string redirectUri, string state)
    {
        var parameters = new Dictionary<string, string>
        {
            ["client_id"] = GogConstants.CLIENT_ID,
            ["redirect_uri"] = redirectUri,
            ["response_type"] = "code",
            ["layout"] = "client2",
            ["state"] = state
        };
        
        var queryString = string.Join("&", parameters.Select(p => $"{Uri.EscapeDataString(p.Key)}={Uri.EscapeDataString(p.Value)}"));
        return $"https://auth.gog.com/auth?{queryString}";
    }

    /// <summary>
    /// Extract authorization code from callback URL
    /// </summary>
    private string? ExtractAuthorizationCode(string callbackUrl, string expectedState)
    {
        try
        {
            var uri = new Uri(callbackUrl);
            var query = uri.Query;
            
            if (string.IsNullOrEmpty(query))
            {
                _logger.LogError("No query parameters in callback URL");
                return null;
            }
            
            var parameters = ParseQueryString(query);
            
            // Check for error
            if (parameters.TryGetValue("error", out var error))
            {
                _logger.LogError("OAuth error: {Error}", error);
                Console.WriteLine($"? OAuth error: {error}");
                return null;
            }
            
            // Validate state parameter
            if (parameters.TryGetValue("state", out var state) && state != expectedState)
            {
                _logger.LogError("Invalid state parameter");
                Console.WriteLine("? Invalid state parameter - possible security issue.");
                return null;
            }
            
            // Extract authorization code
            if (parameters.TryGetValue("code", out var code) && !string.IsNullOrEmpty(code))
            {
                return code;
            }
            
            _logger.LogError("No authorization code in callback URL");
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error parsing callback URL");
            return null;
        }
    }

    /// <summary>
    /// Parse query string into dictionary
    /// </summary>
    private static Dictionary<string, string> ParseQueryString(string query)
    {
        var parameters = new Dictionary<string, string>();
        
        if (query.StartsWith("?"))
            query = query[1..];
        
        var pairs = query.Split('&');
        foreach (var pair in pairs)
        {
            var keyValue = pair.Split('=', 2);
            if (keyValue.Length == 2)
            {
                var key = Uri.UnescapeDataString(keyValue[0]);
                var value = Uri.UnescapeDataString(keyValue[1]);
                parameters[key] = value;
            }
        }
        
        return parameters;
    }

    /// <summary>
    /// Open URL in default browser
    /// </summary>
    private static void OpenBrowser(string url)
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Process.Start(new ProcessStartInfo
                {
                    FileName = url,
                    UseShellExecute = true
                });
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                Process.Start("open", url);
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Process.Start("xdg-open", url);
            }
            else
            {
                throw new PlatformNotSupportedException("Cannot open browser on this platform");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"? Failed to open browser: {ex.Message}");
            Console.WriteLine($"Please manually navigate to: {url}");
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
        }
    }
}