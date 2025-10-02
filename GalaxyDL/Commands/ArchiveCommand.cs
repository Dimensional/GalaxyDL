using System.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using GalaxyDL.Services;
using GalaxyDL.Models;
using System.Text.Json;

namespace GalaxyDL.Commands;

/// <summary>
/// Archive command with subcommands for download, validate, list, and extract
/// This class is currently not used in the simplified Program.cs but contains the structure for future implementation
/// </summary>
public static class ArchiveCommand
{
    // TODO: Implement the full command structure once System.CommandLine binding issues are resolved
    // For now this class serves as documentation for the intended command structure
    
    /*
     * Intended command structure:
     * 
     * galaxydl archive download --game-id 1207658930 --archive-root ./archive --auth-config-path ./auth.json
     * galaxydl archive validate --archive-root ./archive --app-id 1207658930
     * galaxydl archive list --archive-root ./archive --builds
     * galaxydl archive extract --archive-root ./archive --output-dir ./game --game-id 1207658930 --build-id 12345
     */
}