using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.CommandLine;
using GalaxyDL.Services;
using GalaxyDL.Core;
using GalaxyDL.Models;
using Serilog;
using System.Text.Json;

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .WriteTo.File("logs/galaxydl-.txt", 
        rollingInterval: RollingInterval.Day,
        outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    var builder = Host.CreateApplicationBuilder(args);

    // Configure logging
    builder.Services.AddLogging(loggingBuilder =>
    {
        loggingBuilder.ClearProviders();
        loggingBuilder.AddSerilog();
    });

    // Register HTTP client
    builder.Services.AddHttpClient();

    // Register services
    builder.Services.AddSingleton<IGogApiService, GogApiService>();
    builder.Services.AddTransient<IGogArchiverService, GogArchiverService>();
    builder.Services.AddTransient<GogAuthService>();

    var host = builder.Build();
    var serviceProvider = host.Services;

    // Create root command
    var rootCommand = new RootCommand("GOG Galaxy downloader and archiver");
    rootCommand.Description = "A .NET implementation of GOG Galaxy content downloader and archiver, similar to DepotDownloader for Steam.";

    // Add auth command
    var authCommand = new Command("auth", "Authentication operations");
    
    // Add login subcommand
    var loginCommand = new Command("login", "Authenticate with GOG using OAuth2");
    var loginAuthConfigOption = new Option<string>("--auth-config", () => "./auth.json", "Path to save auth config file");
    loginCommand.AddOption(loginAuthConfigOption);
    
    loginCommand.SetHandler(async (authConfig) =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        var authService = serviceProvider.GetRequiredService<GogAuthService>();
        var apiService = serviceProvider.GetRequiredService<IGogApiService>();
        
        try
        {
            Console.WriteLine("🚀 GalaxyDL Authentication");
            Console.WriteLine("==========================");
            Console.WriteLine();
            
            // Initialize API service with the auth config path
            await apiService.InitializeAsync(authConfig);
            
            // Perform authentication
            var success = await authService.AuthenticateAsync(authConfig);
            
            if (success)
            {
                Console.WriteLine();
                Console.WriteLine("🎉 Success! You are now authenticated with GOG.");
                Console.WriteLine($"💾 Credentials saved to: {Path.GetFullPath(authConfig)}");
                Console.WriteLine();
                Console.WriteLine("You can now use other GalaxyDL commands:");
                Console.WriteLine($"  dotnet run -- archive list-builds --game-id 1207658930 --auth-config {authConfig}");
                Console.WriteLine($"  dotnet run -- archive archive-manifests --game-id <game_id> --build-id <build_id> --archive-root ./archive --auth-config {authConfig}");
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine("❌ Authentication failed. Please try again.");
                Environment.Exit(1);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Authentication error");
            Console.WriteLine($"❌ Authentication error: {ex.Message}");
            Environment.Exit(1);
        }
    }, loginAuthConfigOption);
    
    // Add status subcommand
    var statusCommand = new Command("status", "Check authentication status");
    var statusAuthConfigOption = new Option<string>("--auth-config", () => "./auth.json", "Path to auth config file");
    statusCommand.AddOption(statusAuthConfigOption);
    
    statusCommand.SetHandler(async (authConfig) =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        var apiService = serviceProvider.GetRequiredService<IGogApiService>();
        
        try
        {
            if (!File.Exists(authConfig))
            {
                Console.WriteLine($"❌ Auth config file not found: {authConfig}");
                Console.WriteLine("Run 'dotnet run -- auth login' to authenticate.");
                return;
            }
            
            await apiService.InitializeAsync(authConfig);
            
            if (apiService.IsCredentialExpired())
            {
                Console.WriteLine("🔄 Credentials expired, attempting to refresh...");
                var refreshed = await apiService.RefreshCredentialsAsync();
                
                if (refreshed)
                {
                    Console.WriteLine("✅ Credentials refreshed successfully.");
                }
                else
                {
                    Console.WriteLine("❌ Failed to refresh credentials. Please re-authenticate.");
                    Console.WriteLine("Run 'dotnet run -- auth login' to re-authenticate.");
                    return;
                }
            }
            else
            {
                Console.WriteLine("✅ You are authenticated with GOG.");
            }
            
            Console.WriteLine($"📁 Auth config: {Path.GetFullPath(authConfig)}");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error checking auth status");
            Console.WriteLine($"❌ Error: {ex.Message}");
        }
    }, statusAuthConfigOption);
    
    authCommand.AddCommand(loginCommand);
    authCommand.AddCommand(statusCommand);

    // Add archive command with basic functionality
    var archiveCommand = new Command("archive", "Archive operations");
    
    // Add test subcommand
    var testCommand = new Command("test", "Test the application");
    testCommand.SetHandler(() =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        logger.LogInformation("Test command executed successfully!");
        Console.WriteLine("✅ GalaxyDL is working! Test command executed.");
        Console.WriteLine("🔨 Full archive functionality is now implemented and ready for testing.");
        Console.WriteLine();
        Console.WriteLine("Next steps:");
        Console.WriteLine("1. Run 'dotnet run -- auth login' to authenticate with GOG");
        Console.WriteLine("2. Run 'dotnet run -- archive list-builds --game-id 1207658930' to test build listing");
    });
    
    // Add test command for specific repository IDs
    var testRepoCommand = new Command("test-repo", "Test specific repository ID access");
    var repoIdOption = new Option<string>("--repo-id", "Repository ID to test") { IsRequired = true };
    var repoGenOption = new Option<int>("--generation", "Generation (1 or 2)") { IsRequired = true };
    var testGameIdOption = new Option<string>("--game-id", () => "1207658930", "Game ID for testing");
    var testPlatformOption = new Option<string>("--platform", () => "windows", "Platform for testing");
    var testRepoAuthConfigOption = new Option<string>("--auth-config", () => "./auth.json", "Path to auth config file");
    
    testRepoCommand.AddOption(repoIdOption);
    testRepoCommand.AddOption(repoGenOption);
    testRepoCommand.AddOption(testGameIdOption);
    testRepoCommand.AddOption(testPlatformOption);
    testRepoCommand.AddOption(testRepoAuthConfigOption);
    
    testRepoCommand.SetHandler(async (repoId, generation, gameId, platform, authConfig) =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        var archiverService = serviceProvider.GetRequiredService<IGogArchiverService>();
        
        try
        {
            if (!File.Exists(authConfig))
            {
                Console.WriteLine($"❌ Auth config file not found: {authConfig}");
                Console.WriteLine("Run 'dotnet run -- auth login' to authenticate first.");
                return;
            }
            
            Console.WriteLine($"🧪 Testing Repository ID: {repoId}");
            Console.WriteLine($"📋 Generation: V{generation}");
            Console.WriteLine($"🎮 Game: {gameId}, Platform: {platform}");
            Console.WriteLine();
            
            // Initialize archiver
            var tempArchive = Path.Combine(Environment.GetEnvironmentVariable("TEMP") ?? "./temp", "galaxy-test");
            await archiverService.InitializeAsync(tempArchive, authConfig);
            
            // Test archiving this specific repository ID as if it were a build ID
            var result = await archiverService.ArchiveBuildAndDepotManifestsOnlyAsync(gameId, repoId, new List<string> { platform });
            
            Console.WriteLine("📊 Test Results:");
            Console.WriteLine($"   Build manifests: {result.ManifestsArchived}");
            Console.WriteLine($"   Depot manifests: {result.DepotManifestsArchived}");
            
            if (result.Errors.Any())
            {
                Console.WriteLine();
                Console.WriteLine("⚠️  Errors:");
                foreach (var error in result.Errors)
                {
                    Console.WriteLine($"   - {error}");
                }
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine("✅ Repository manifest retrieved successfully!");
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error testing repository");
            Console.WriteLine($"❌ Error: {ex.Message}");
        }
    }, repoIdOption, repoGenOption, testGameIdOption, testPlatformOption, testRepoAuthConfigOption);
    
    // Add list-builds command
    var listBuildsCommand = new Command("list-builds", "List available builds for a game");
    var gameIdOption = new Option<string>("--game-id", "GOG Game ID") { IsRequired = true };
    var listBuildsAuthConfigOption = new Option<string>("--auth-config", () => "./auth.json", "Path to auth config file");
    var platformsOption = new Option<List<string>>("--platforms", () => new List<string> { "windows", "osx" }, "Platforms to query (windows, osx, linux)");
    var includeLinuxOption = new Option<bool>("--include-linux", "Include Linux platform (rare - most games don't have Linux manifests)");
    var generationOption = new Option<int?>("--generation", "API generation (1 or 2)");
    
    listBuildsCommand.AddOption(gameIdOption);
    listBuildsCommand.AddOption(listBuildsAuthConfigOption);
    listBuildsCommand.AddOption(platformsOption);
    listBuildsCommand.AddOption(includeLinuxOption);
    listBuildsCommand.AddOption(generationOption);
    
    listBuildsCommand.SetHandler(async (gameId, authConfig, platforms, includeLinux, generation) =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        var archiverService = serviceProvider.GetRequiredService<IGogArchiverService>();
        
        try
        {
            if (!File.Exists(authConfig))
            {
                Console.WriteLine($"❌ Auth config file not found: {authConfig}");
                Console.WriteLine("Run 'dotnet run -- auth login' to authenticate first.");
                return;
            }
            
            logger.LogInformation("Listing builds for game {GameId}", gameId);
            
            // Add Linux to platforms if explicitly requested
            if (includeLinux && !platforms.Contains("linux"))
            {
                platforms.Add("linux");
                Console.WriteLine("🐧 Including Linux platform (Note: Linux GOG Galaxy manifests are very rare)");
            }
            
            // Initialize with a temporary archive directory
            var tempArchive = Path.Combine(Environment.GetEnvironmentVariable("TEMP") ?? "./temp", "galaxy-test");
            await archiverService.InitializeAsync(tempArchive, authConfig);
            
            var result = await archiverService.ListBuildsAsync(gameId, platforms, generation);
            
            if (result.Error != null)
            {
                Console.WriteLine($"❌ Error: {result.Error}");
                return;
            }
            
            Console.WriteLine($"🎯 Found {result.Builds.Count} builds for game {gameId}:");
            Console.WriteLine();
            
            // Group builds by platform for better display
            var buildsByPlatform = result.Builds.GroupBy(b => b.Platform).OrderBy(g => g.Key);
            
            foreach (var platformGroup in buildsByPlatform)
            {
                var platform = platformGroup.Key;
                var platformSymbol = platform switch
                {
                    "windows" => "🪟",
                    "osx" => "🍎", 
                    "linux" => "🐧",
                    _ => "❓"
                };
                var platformName = platform switch
                {
                    "windows" => "Windows",
                    "osx" => "macOS",
                    "linux" => "Linux", 
                    _ => platform
                };
                
                Console.WriteLine($"{platformSymbol} {platformName} ({platformGroup.Count()} builds):");
                
                foreach (var build in platformGroup.Take(5)) // Limit to first 5 per platform
                {
                    Console.WriteLine($"  📦 Build ID: {build.BuildId}");
                    Console.WriteLine($"     Version: {build.VersionName}");
                    Console.WriteLine($"     Legacy: {build.Legacy}");
                    Console.WriteLine($"     Published: {build.DatePublished}");
                    Console.WriteLine($"     Generation: V{build.GenerationQueried}");
                    Console.WriteLine();
                }
                
                if (platformGroup.Count() > 5)
                {
                    Console.WriteLine($"     ... and {platformGroup.Count() - 5} more {platformName} builds");
                    Console.WriteLine();
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error listing builds");
            Console.WriteLine($"❌ Error: {ex.Message}");
        }
    }, gameIdOption, listBuildsAuthConfigOption, platformsOption, includeLinuxOption, generationOption);
    
    // Add archive-manifests command
    var archiveManifestsCommand = new Command("archive-manifests", "Archive build and depot manifests (manifests-only mode)");
    var archiveManifestsGameIdOption = new Option<string>("--game-id", "GOG Game ID") { IsRequired = true };
    var archiveManifestsAuthConfigOption = new Option<string>("--auth-config", () => "./auth.json", "Path to auth config file");
    var buildIdOption = new Option<string>("--build-id", "Build ID to archive") { IsRequired = true };
    var archiveRootOption = new Option<string>("--archive-root", "Archive root directory") { IsRequired = true };
    var archiveManifestsPlatformsOption = new Option<List<string>>("--platforms", () => new List<string> { "windows" }, "Platforms to query");
    
    archiveManifestsCommand.AddOption(archiveManifestsGameIdOption);
    archiveManifestsCommand.AddOption(archiveManifestsAuthConfigOption);
    archiveManifestsCommand.AddOption(buildIdOption);
    archiveManifestsCommand.AddOption(archiveRootOption);
    archiveManifestsCommand.AddOption(archiveManifestsPlatformsOption);
    
    archiveManifestsCommand.SetHandler(async (gameId, authConfig, buildId, archiveRoot, platforms) =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        var archiverService = serviceProvider.GetRequiredService<IGogArchiverService>();
        
        try
        {
            if (!File.Exists(authConfig))
            {
                Console.WriteLine($"❌ Auth config file not found: {authConfig}");
                Console.WriteLine("Run 'dotnet run -- auth login' to authenticate first.");
                return;
            }
            
            logger.LogInformation("Archiving manifests for {GameId}/{BuildId}", gameId, buildId);
            Console.WriteLine($"🔽 Archiving manifests for game {gameId}, build {buildId}");
            Console.WriteLine($"📁 Archive root: {archiveRoot}");
            Console.WriteLine();
            
            await archiverService.InitializeAsync(archiveRoot, authConfig);
            
            var result = await archiverService.ArchiveBuildAndDepotManifestsOnlyAsync(gameId, buildId, platforms);
            
            Console.WriteLine("📊 Archive Results:");
            Console.WriteLine($"   Build manifests: {result.ManifestsArchived}");
            Console.WriteLine($"   Depot manifests: {result.DepotManifestsArchived}");
            
            if (result.Errors.Any())
            {
                Console.WriteLine();
                Console.WriteLine("⚠️  Errors:");
                foreach (var error in result.Errors)
                {
                    Console.WriteLine($"   - {error}");
                }
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine("✅ Manifests archived successfully!");
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error archiving manifests");
            Console.WriteLine($"❌ Error: {ex.Message}");
        }
    }, archiveManifestsGameIdOption, archiveManifestsAuthConfigOption, buildIdOption, archiveRootOption, archiveManifestsPlatformsOption);
    
    // Add archive stats command
    var statsCommand = new Command("stats", "Show archive statistics");
    var statsArchiveRootOption = new Option<string>("--archive-root", "Archive root directory") { IsRequired = true };
    statsCommand.AddOption(statsArchiveRootOption);
    
    statsCommand.SetHandler(async (archiveRoot) =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        var archiverService = serviceProvider.GetRequiredService<IGogArchiverService>();
        
        try
        {
            await archiverService.InitializeAsync(archiveRoot);
            var stats = await archiverService.GetArchiveStatsAsync();
            
            Console.WriteLine("📊 Archive Statistics:");
            Console.WriteLine(JsonSerializer.Serialize(stats, new JsonSerializerOptions { WriteIndented = true }));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error getting archive stats");
            Console.WriteLine($"❌ Error: {ex.Message}");
        }
    }, statsArchiveRootOption);
    
    // Add version command instead of option
    var versionCommand = new Command("version", "Show version information");
    versionCommand.SetHandler(() =>
    {
        Console.WriteLine("GalaxyDL v1.0.0");
        Console.WriteLine("A .NET implementation of GOG Galaxy content downloader and archiver");
        Console.WriteLine("https://github.com/your-repo/GalaxyDL");
    });
    
    archiveCommand.AddCommand(testCommand);
    archiveCommand.AddCommand(testRepoCommand);
    archiveCommand.AddCommand(listBuildsCommand);
    archiveCommand.AddCommand(archiveManifestsCommand);
    archiveCommand.AddCommand(statsCommand);
    
    rootCommand.AddCommand(authCommand);
    rootCommand.AddCommand(archiveCommand);
    rootCommand.AddCommand(versionCommand);

    // Configure root command handler for when no subcommand is provided
    rootCommand.SetHandler(() =>
    {
        Console.WriteLine("🚀 GalaxyDL - GOG Galaxy Downloader and Archiver");
        Console.WriteLine("================================================");
        Console.WriteLine();
        Console.WriteLine("Available commands:");
        Console.WriteLine("  auth login                      - Authenticate with GOG using OAuth2 (browser-based)");
        Console.WriteLine("  auth status                     - Check authentication status");
        Console.WriteLine("  archive test                    - Test the application setup");
        Console.WriteLine("  archive list-builds             - List available builds for a game");
        Console.WriteLine("  archive archive-manifests       - Archive build and depot manifests");
        Console.WriteLine("  archive stats                   - Show archive statistics");
        Console.WriteLine("  version                         - Show version information");
        Console.WriteLine();
        Console.WriteLine("🔐 Authentication:");
        Console.WriteLine("   GalaxyDL now includes automatic OAuth2 authentication!");
        Console.WriteLine("   Simply run 'dotnet run -- auth login' to get started.");
        Console.WriteLine();
        Console.WriteLine("🚀 Features Ready:");
        Console.WriteLine("   ✅ Automatic OAuth2 authentication with browser");
        Console.WriteLine("   ✅ Build discovery (V1 and V2 APIs)");
        Console.WriteLine("   ✅ Manifest downloading and archiving");
        Console.WriteLine("   ✅ Archive management and statistics");
        Console.WriteLine();
        Console.WriteLine("Use 'dotnet run -- <command> --help' for more information about a command.");
        Console.WriteLine();
        Console.WriteLine("Quick Start:");
        Console.WriteLine("  1. dotnet run -- auth login");
        Console.WriteLine("  2. dotnet run -- archive list-builds --game-id 1207658930");
        Console.WriteLine("  3. dotnet run -- archive archive-manifests --game-id 1207658930 --build-id <build_id> --archive-root ./archive");
    });

    // Parse and invoke
    var exitCode = await rootCommand.InvokeAsync(args);
    
    await host.StopAsync();
    return exitCode;
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application terminated unexpectedly");
    return 1;
}
finally
{
    Log.CloseAndFlush();
}
