using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace GalaxyDL.Core;

/// <summary>
/// Utility functions for GOG Galaxy operations
/// </summary>
public static class GogUtils
{
    /// <summary>
    /// Calculate galaxy path for chunk storage (ab/cd/abcdef...)
    /// </summary>
    public static string GalaxyPath(string hash)
    {
        if (string.IsNullOrEmpty(hash) || hash.Length < 4)
            return hash;
            
        return $"{hash[..2]}/{hash[2..4]}/{hash}";
    }

    /// <summary>
    /// Merge URL with parameters
    /// </summary>
    public static string MergeUrlWithParams(string urlFormat, Dictionary<string, object> parameters)
    {
        var url = urlFormat;
        
        foreach (var param in parameters)
        {
            var placeholder = $"{{{param.Key}}}";
            if (url.Contains(placeholder))
            {
                url = url.Replace(placeholder, param.Value.ToString());
            }
        }
        
        return url;
    }

    /// <summary>
    /// Calculate MD5 hash of data
    /// </summary>
    public static string CalculateMd5(byte[] data)
    {
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Calculate SHA1 hash of data
    /// </summary>
    public static string CalculateSha1(byte[] data)
    {
        using var sha1 = SHA1.Create();
        var hash = sha1.ComputeHash(data);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Calculate SHA256 hash of data
    /// </summary>
    public static string CalculateSha256(byte[] data)
    {
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(data);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Decompress zlib data
    /// </summary>
    public static byte[] DecompressZlib(byte[] compressedData)
    {
        using var input = new MemoryStream(compressedData);
        using var deflate = new DeflateStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        
        deflate.CopyTo(output);
        return output.ToArray();
    }

    /// <summary>
    /// Decompress gzip data
    /// </summary>
    public static byte[] DecompressGzip(byte[] compressedData)
    {
        using var input = new MemoryStream(compressedData);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        
        gzip.CopyTo(output);
        return output.ToArray();
    }

    /// <summary>
    /// Try to decompress data (auto-detect format)
    /// </summary>
    public static (byte[] data, bool wasCompressed) TryDecompress(byte[] data)
    {
        if (data.Length < 2)
            return (data, false);

        try
        {
            // Check for gzip signature
            if (data[0] == 0x1f && data[1] == 0x8b)
            {
                return (DecompressGzip(data), true);
            }
            
            // Check for zlib signature
            if (data[0] == 0x78)
            {
                return (DecompressZlib(data), true);
            }
        }
        catch
        {
            // If decompression fails, return original data
        }

        return (data, false);
    }

    /// <summary>
    /// Parse JSON from bytes with optional decompression
    /// </summary>
    public static T? ParseJsonWithDecompression<T>(byte[] data) where T : class
    {
        var (decompressedData, _) = TryDecompress(data);
        var json = Encoding.UTF8.GetString(decompressedData);
        return JsonSerializer.Deserialize<T>(json, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true
        });
    }

    /// <summary>
    /// Parse JSON dictionary from bytes with optional decompression
    /// </summary>
    public static Dictionary<string, object>? ParseJsonDictionaryWithDecompression(byte[] data)
    {
        var (decompressedData, _) = TryDecompress(data);
        var json = Encoding.UTF8.GetString(decompressedData);
        return JsonSerializer.Deserialize<Dictionary<string, object>>(json, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true
        });
    }

    /// <summary>
    /// Generate user agent string
    /// </summary>
    public static string GenerateUserAgent(string version = "1.0.0")
    {
        return $"{GogConstants.USER_AGENT_BASE}/{version} {GogConstants.HEROIC_REFERENCE}";
    }

    /// <summary>
    /// Ensure directory exists
    /// </summary>
    public static void EnsureDirectoryExists(string path)
    {
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }
    }

    /// <summary>
    /// Get relative path from base directory
    /// </summary>
    public static string GetRelativePath(string basePath, string fullPath)
    {
        var baseUri = new Uri(Path.GetFullPath(basePath) + Path.DirectorySeparatorChar);
        var fullUri = new Uri(Path.GetFullPath(fullPath));
        return Uri.UnescapeDataString(baseUri.MakeRelativeUri(fullUri).ToString().Replace('/', Path.DirectorySeparatorChar));
    }

    /// <summary>
    /// Safe file write with atomic operation
    /// </summary>
    public static async Task SafeWriteAllBytesAsync(string filePath, byte[] data, CancellationToken cancellationToken = default)
    {
        var tempPath = filePath + ".tmp";
        
        try
        {
            await File.WriteAllBytesAsync(tempPath, data, cancellationToken);
            
            // Atomic move
            if (File.Exists(filePath))
                File.Delete(filePath);
                
            File.Move(tempPath, filePath);
        }
        finally
        {
            if (File.Exists(tempPath))
                File.Delete(tempPath);
        }
    }

    /// <summary>
    /// Safe JSON write with atomic operation
    /// </summary>
    public static async Task SafeWriteJsonAsync<T>(string filePath, T data, CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(data, new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        
        var bytes = Encoding.UTF8.GetBytes(json);
        await SafeWriteAllBytesAsync(filePath, bytes, cancellationToken);
    }

    /// <summary>
    /// Read JSON file with error handling
    /// </summary>
    public static async Task<T?> ReadJsonAsync<T>(string filePath, CancellationToken cancellationToken = default) where T : class
    {
        if (!File.Exists(filePath))
            return null;

        try
        {
            var json = await File.ReadAllTextAsync(filePath, cancellationToken);
            return JsonSerializer.Deserialize<T>(json, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                PropertyNameCaseInsensitive = true
            });
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Format bytes as human-readable size
    /// </summary>
    public static string FormatBytes(long bytes)
    {
        string[] suffixes = { "B", "KB", "MB", "GB", "TB" };
        int counter = 0;
        decimal number = bytes;
        
        while (Math.Round(number / 1024) >= 1)
        {
            number /= 1024;
            counter++;
        }
        
        return $"{number:n2} {suffixes[counter]}";
    }

    /// <summary>
    /// Validate file integrity by comparing size and optional hash
    /// </summary>
    public static async Task<bool> ValidateFileIntegrityAsync(string filePath, long? expectedSize = null, string? expectedHash = null, string hashAlgorithm = "MD5", CancellationToken cancellationToken = default)
    {
        if (!File.Exists(filePath))
            return false;

        var fileInfo = new FileInfo(filePath);
        
        // Check size if provided
        if (expectedSize.HasValue && fileInfo.Length != expectedSize.Value)
            return false;

        // Check hash if provided
        if (!string.IsNullOrEmpty(expectedHash))
        {
            var data = await File.ReadAllBytesAsync(filePath, cancellationToken);
            var actualHash = hashAlgorithm.ToUpperInvariant() switch
            {
                "MD5" => CalculateMd5(data),
                "SHA1" => CalculateSha1(data),
                "SHA256" => CalculateSha256(data),
                _ => throw new ArgumentException($"Unsupported hash algorithm: {hashAlgorithm}")
            };
            
            return string.Equals(actualHash, expectedHash, StringComparison.OrdinalIgnoreCase);
        }

        return true;
    }
}

/// <summary>
/// Extension methods for dictionary access
/// </summary>
public static class DictionaryExtensions
{
    /// <summary>
    /// Get value or default from dictionary
    /// </summary>
    public static TValue GetValueOrDefault<TKey, TValue>(this Dictionary<TKey, object> dictionary, TKey key, TValue defaultValue = default!)
        where TKey : notnull
    {
        if (dictionary.TryGetValue(key, out var value))
        {
            if (value is TValue typedValue)
                return typedValue;
            
            // Try to convert
            try
            {
                if (typeof(TValue) == typeof(string))
                    return (TValue)(object)value.ToString()!;
                
                return (TValue)Convert.ChangeType(value, typeof(TValue));
            }
            catch
            {
                return defaultValue;
            }
        }
        
        return defaultValue;
    }
}