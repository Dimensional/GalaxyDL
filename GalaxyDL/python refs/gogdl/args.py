# Initialize argparse module and return arguments
import argparse
from multiprocessing import cpu_count


def init_parser():
    parser = argparse.ArgumentParser(
        description="GOG downloader for Heroic Games Launcher"
    )

    parser.add_argument(
        "--version",
        "-v",
        dest="display_version",
        action="store_true",
        help="Display GOGDL version",
    )

    parser.add_argument("--auth-config-path", dest="auth_config_path",
                        help="Path to json file where tokens will be stored (required for most operations)")

    subparsers = parser.add_subparsers(dest="command")

    import_parser = subparsers.add_parser(
        "import", help="Show data about game in the specified path"
    )
    import_parser.add_argument("path")

    # REDIST DOWNLOAD

    redist_download_parser = subparsers.add_parser("redist", aliases=["dependencies"],
                                                   help="Download specified dependencies to provided location")
    redist_download_parser.add_argument("--auth-config-path", dest="auth_config_path", required=True,
                                       help="Path to json file where tokens will be stored")
    redist_download_parser.add_argument("--ids", help="Coma separated ids")
    redist_download_parser.add_argument("--path", help="Location where to download the files")
    redist_download_parser.add_argument("--print-manifest", action="store_true", help="Prints manifest to stdout and exits")
    redist_download_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=cpu_count(),
        help="Specify number of worker threads, by default number of CPU threads",
    )


    # AUTH

    auth_parser = subparsers.add_parser("auth", help="Manage authorization")
    auth_parser.add_argument("--auth-config-path", dest="auth_config_path", required=True,
                            help="Path to json file where tokens will be stored")
    auth_parser.add_argument("--client-id", dest="client_id")
    auth_parser.add_argument("--client-secret", dest="client_secret")
    auth_parser.add_argument("--code", dest="authorization_code",
                             help="Pass authorization code (use for login), when passed client-id and secret are ignored")

    # DOWNLOAD

    download_parser = subparsers.add_parser(
        "download", aliases=["repair", "update"], help="Download/update/repair game"
    )
    download_parser.add_argument("--auth-config-path", dest="auth_config_path", required=True,
                                help="Path to json file where tokens will be stored")
    download_parser.add_argument("id", help="Game id")
    download_parser.add_argument("--lang", "-l", help="Specify game language")
    download_parser.add_argument(
        "--build", "-b", dest="build", help="Specify buildId"
    )
    download_parser.add_argument(
        "--path", "-p", dest="path", help="Specify download path", required=True
    )
    download_parser.add_argument("--support", dest="support_path", help="Specify path where support files should be stored, by default they are put into game dir")
    download_parser.add_argument(
        "--platform",
        "--os",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
    )
    download_parser.add_argument(
        "--with-dlcs", dest="dlcs", action="store_true", help="Should download all dlcs"
    )
    download_parser.add_argument(
        "--skip-dlcs", dest="dlcs", action="store_false", help="Should skip all dlcs"
    )
    download_parser.add_argument(
        "--dlcs",
        dest="dlcs_list",
        default=[],
        help="List of dlc ids to download (separated by coma)",
    )
    download_parser.add_argument(
        "--dlc-only", dest="dlc_only", action="store_true", help="Download only DLC"
    )
    download_parser.add_argument("--branch", help="Choose build branch to use")
    download_parser.add_argument("--password", help="Password to access other branches")
    download_parser.add_argument("--force-gen", choices=["1", "2"], dest="force_generation", help="Force specific manifest generation (FOR DEBUGGING)")
    download_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=cpu_count(),
        help="Specify number of worker threads, by default number of CPU threads",
    )

    # SIZE CALCULATING, AND OTHER MANIFEST INFO

    calculate_size_parser = subparsers.add_parser(
        "info", help="Calculates estimated download size and list of DLCs"
    )
    calculate_size_parser.add_argument("--auth-config-path", dest="auth_config_path", required=True,
                                      help="Path to json file where tokens will be stored")

    calculate_size_parser.add_argument(
        "--with-dlcs",
        dest="dlcs",
        action="store_true",
        help="Should download all dlcs",
    )
    calculate_size_parser.add_argument(
        "--skip-dlcs", dest="dlcs", action="store_false", help="Should skip all dlcs"
    )
    calculate_size_parser.add_argument(
        "--dlcs",
        dest="dlcs_list",
        help="Coma separated list of dlc ids to download",
    )
    calculate_size_parser.add_argument(
        "--dlc-only", dest="dlc_only", action="store_true", help="Download only DLC"
    )
    calculate_size_parser.add_argument("id")
    calculate_size_parser.add_argument(
        "--platform",
        "--os",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
    )
    calculate_size_parser.add_argument(
        "--build", "-b", dest="build", help="Specify buildId"
    )
    calculate_size_parser.add_argument("--lang", "-l", help="Specify game language")
    calculate_size_parser.add_argument("--branch", help="Choose build branch to use")
    calculate_size_parser.add_argument("--password", help="Password to access other branches")
    calculate_size_parser.add_argument("--force-gen", choices=["1", "2"], dest="force_generation", help="Force specific manifest generation (FOR DEBUGGING)")
    calculate_size_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=cpu_count(),
        help="Specify number of worker threads, by default number of CPU threads",
    )

    # LAUNCH

    launch_parser = subparsers.add_parser(
        "launch", help="Launch the game in specified path", add_help=False
    )
    launch_parser.add_argument("path")
    launch_parser.add_argument("id")
    launch_parser.add_argument(
        "--platform",
        "--os",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
        required=True,
    )
    launch_parser.add_argument("--prefer-task", dest="preferred_task", default=None, help="Select playTask index to be run")
    launch_parser.add_argument(
        "--no-wine", action="store_true", dest="dont_use_wine", default=False
    )
    launch_parser.add_argument("--wine", dest="wine", help="Specify wine bin path")
    launch_parser.add_argument("--wine-prefix", dest="wine_prefix")
    launch_parser.add_argument("--wrapper", dest="wrapper")
    launch_parser.add_argument(
        "--override-exe", dest="override_exe", help="Override executable to be run"
    )

    # SAVES

    save_parser = subparsers.add_parser("save-sync", help="Sync game saves")
    save_parser.add_argument("--auth-config-path", dest="auth_config_path", required=True,
                            help="Path to json file where tokens will be stored")
    save_parser.add_argument("path", help="Path to sync files")
    save_parser.add_argument("id", help="Game id")
    save_parser.add_argument(
        "--ts", dest="timestamp", help="Last sync timestamp", required=True
    )
    save_parser.add_argument("--name", dest="dirname", default="__default")
    save_parser.add_argument(
        "--skip-download", dest="prefered_action", action="store_const", const="upload"
    )
    save_parser.add_argument(
        "--skip-upload", dest="prefered_action", action="store_const", const="download"
    )
    save_parser.add_argument(
        "--force-upload",
        dest="prefered_action",
        action="store_const",
        const="forceupload",
    )
    save_parser.add_argument(
        "--force-download",
        dest="prefered_action",
        action="store_const",
        const="forcedownload",
    )

    save_parser.add_argument(
        "--os",
        "--platform",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
        required=True,
    )

    # SAVES CLEAR

    clear_parser = subparsers.add_parser("save-clear", help="Clear cloud game saves")
    clear_parser.add_argument("--auth-config-path", dest="auth_config_path", required=True,
                             help="Path to json file where tokens will be stored")
    clear_parser.add_argument("path", help="Path to sync files")
    clear_parser.add_argument("id", help="Game id")
    clear_parser.add_argument("--name", dest="dirname", default="__default")

    clear_parser.add_argument(
        "--os",
        "--platform",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
        required=True,
    )

    # Languages

    locale_parser = subparsers.add_parser("lang-match", help="Query GOG language data for given locale code/name")
    locale_parser.add_argument("language", help="Language query to match")

    # ARCHIVE GOG GALAXY CDN CONTENT - MAIN COMMAND WITH SUBCOMMANDS

    archive_parser = subparsers.add_parser("archive", help="Archive and validate GOG Galaxy CDN content")
    archive_subparsers = archive_parser.add_subparsers(dest="archive_command", help="Archive operations")
    
    # ARCHIVE DOWNLOAD - Requires authentication
    archive_download_parser = archive_subparsers.add_parser("download", help="Download and archive game content (requires auth)")
    archive_download_parser.add_argument("--auth-config-path", dest="auth_config_path", required=True,
                                        help="Path to json file where tokens will be stored")
    archive_download_parser.add_argument("--game-id", required=True, help="GOG Game ID to archive")
    archive_download_parser.add_argument("--build-id", help="Specific Build ID to archive (optional)")
    archive_download_parser.add_argument("--manifest-id", help="Specific Manifest ID to archive (requires --build-id)")
    archive_download_parser.add_argument("--archive-root", required=True, help="Root directory for archive storage")
    archive_download_parser.add_argument("--platforms", nargs='+', default=['all'], 
                                        choices=["windows", "osx", "linux", "all"], help="Platforms to archive (use 'all' for all platforms)")
    archive_download_parser.add_argument("--languages", nargs='+', default=['en'], help="Languages to archive")
    archive_download_parser.add_argument("--list-builds", action='store_true', help="List available builds for the game")
    archive_download_parser.add_argument("--generation", type=int, choices=[1, 2], help="API generation to query (1=V1 builds, 2=V2 builds, omit for both)")
    archive_download_parser.add_argument("--list-manifests", action='store_true', help="List manifests in a build (requires --build-id)")
    archive_download_parser.add_argument("--dry-run", action='store_true', help="Preview what would be archived without downloading content")
    archive_download_parser.add_argument("--manifests-only", action='store_true', help="Download only build and depot manifests, skip chunks/blobs")
    archive_download_parser.add_argument("--max-workers", type=int, default=4, help="Number of download threads (default: 4)")
    archive_download_parser.add_argument("--validate-existing", action='store_true', help="Validate existing chunks/blobs before download")
    
    # Repository mode arguments
    archive_download_parser.add_argument("--repository", help="Repository ID for repository-based download (alternative to --build-id)")
    archive_download_parser.add_argument("-v1", "--v1", action='store_true', help="Use V1 API for repository download")
    archive_download_parser.add_argument("-v2", "--v2", action='store_true', help="Use V2 API for repository download")
    
    # ARCHIVE VALIDATE - No authentication required
    archive_validate_parser = archive_subparsers.add_parser("validate", help="Validate archived content integrity (no auth required)")
    archive_validate_parser.add_argument("--archive-root", required=True, help="Root directory of archive to validate")
    archive_validate_parser.add_argument("--app-id", help="Application ID to validate (e.g., 1207658930)")
    archive_validate_parser.add_argument("--build-id", help="Build ID to validate (e.g., 3161)")
    archive_validate_parser.add_argument("--chunk-validation", action='store_true', help="Validate V2 chunks with hash verification")
    archive_validate_parser.add_argument("--blob-validation", action='store_true', help="Validate V1 blobs with comprehensive file checking")
    archive_validate_parser.add_argument("--multithreaded", action='store_true', help="Use multithreaded validation for faster processing")
    archive_validate_parser.add_argument("--threads", type=int, help="Number of validation threads (auto-detect if not specified)")
    archive_validate_parser.add_argument("--repair", action='store_true', help="Mark corrupted files for re-download (requires --auth-config-path)")
    archive_validate_parser.add_argument("--summary-only", action='store_true', help="Show summary without detailed file validation")
    
    # ARCHIVE LIST - No authentication required
    archive_list_parser = archive_subparsers.add_parser("list", help="List archived content (no auth required)")
    archive_list_parser.add_argument("--archive-root", required=True, help="Root directory of archive to examine")
    archive_list_parser.add_argument("--builds", action='store_true', help="List all archived builds")
    archive_list_parser.add_argument("--chunks", action='store_true', help="List V2 chunks with statistics")
    archive_list_parser.add_argument("--blobs", action='store_true', help="List V1 blobs with sizes")
    archive_list_parser.add_argument("--manifests", action='store_true', help="List all manifests")
    archive_list_parser.add_argument("--game-id", help="Filter by specific game ID")
    archive_list_parser.add_argument("--detailed", action='store_true', help="Show detailed information")
    
    # ARCHIVE EXTRACT - No authentication required
    archive_extract_parser = archive_subparsers.add_parser("extract", help="Extract archived game content to playable form (no auth required)")
    archive_extract_parser.add_argument("--archive-root", required=True, help="Root directory of archive to extract from")
    archive_extract_parser.add_argument("--output-dir", required=True, help="Directory where extracted game files will be placed")
    archive_extract_parser.add_argument("--game-id", required=True, help="Game ID to extract (e.g., 1207658930)")
    archive_extract_parser.add_argument("--build-id", required=True, help="Build ID to extract (e.g., 3161 or 48906206523382029)")
    archive_extract_parser.add_argument("--platform", default="windows", choices=["windows", "osx", "linux"], help="Platform to extract (default: windows)")
    archive_extract_parser.add_argument("--language", default="en", help="Language to extract (default: en)")
    archive_extract_parser.add_argument("--dry-run", action='store_true', help="Show what would be extracted without actually extracting")
    archive_extract_parser.add_argument("--verify-checksums", action='store_true', help="Verify file checksums after extraction")
    archive_extract_parser.add_argument("--overwrite", action='store_true', help="Overwrite existing files in output directory")
    archive_extract_parser.add_argument("--max-workers", type=int, default=4, help="Number of extraction threads (default: 4)")
    
    # TODO: ARCHIVE REPAIR - Will require authentication when implemented
    # archive_repair_parser = archive_subparsers.add_parser("repair", help="Repair corrupted archive content (requires auth)")
    # Currently not implemented - would validate archive and re-download corrupted content

    return parser.parse_known_args()
