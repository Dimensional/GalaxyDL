#!/usr/bin/env python3
from multiprocessing import freeze_support
import gogdl.args as args
from gogdl.dl.managers import manager
from gogdl.dl.managers import dependencies
import gogdl.api as api
import gogdl.imports as imports
import gogdl.launch as launch
import gogdl.languages as languages
import gogdl.saves as saves
import gogdl.auth as auth
import gogdl.archiver as archiver
from gogdl import version as gogdl_version
import json
import logging


def display_version():
    print(f"{gogdl_version}")


def archive_download(arguments, unknown_arguments):
    """Handle archive download subcommand - requires authentication"""
    galaxy_archiver = archiver.GOGGalaxyArchiver(arguments.archive_root, arguments.auth_config_path)
    
    # Handle 'all' platforms special case (default is already ['all'] from args.py)
    if 'all' in arguments.platforms:
        platforms = ['windows', 'osx', 'linux']
    else:
        platforms = arguments.platforms
        
    languages = arguments.languages if hasattr(arguments, 'languages') and arguments.languages else ['en']
    
    # Handle listing commands
    if arguments.list_builds:
        generation = getattr(arguments, 'generation', None)
        builds = galaxy_archiver.list_builds(arguments.game_id, platforms, generation)
        print(json.dumps(builds, indent=2))
        return
    
    if arguments.list_manifests:
        if not arguments.build_id:
            print("Error: --list-manifests requires --build-id")
            return
        manifests = galaxy_archiver.list_manifests(arguments.game_id, arguments.build_id, platforms)
        print(json.dumps(manifests, indent=2))
        return
    
    # Handle dry-run mode
    dry_run = hasattr(arguments, 'dry_run') and arguments.dry_run
    manifests_only = hasattr(arguments, 'manifests_only') and arguments.manifests_only
    
    if dry_run:
        print("=== DRY RUN MODE - No content will be downloaded ===")
    elif manifests_only:
        print("=== MANIFESTS-ONLY MODE - Only manifests will be downloaded ===")
    
    # Handle archiving commands
    if arguments.repository:
        # Repository mode
        repository_id = arguments.repository
        
        # Determine API version for repository download
        if arguments.v1 and arguments.v2:
            print("Error: Cannot specify both -v1 and -v2 flags")
            return
        elif arguments.v1:
            repository_version = 1
        elif arguments.v2:
            repository_version = 2
        else:
            print("Error: Repository mode requires either -v1 or -v2 flag")
            return
        
        print(f"Repository mode: downloading repository {repository_id} using V{repository_version} API")
        
        if dry_run:
            print(f"Would archive repository {repository_id} for game {arguments.game_id} using V{repository_version} API")
            return
        elif manifests_only:
            print(f"Archiving manifests for repository {repository_id} for game {arguments.game_id} using V{repository_version} API")
            results = galaxy_archiver.archive_repository_and_depot_manifests_only(arguments.game_id, repository_id, repository_version, platforms)
        else:
            results = galaxy_archiver.archive_build(arguments.game_id, repository_id, platforms, languages, None, repository_version)
    elif arguments.manifest_id:
        if not arguments.build_id:
            print("Error: --manifest-id requires --build-id")
            return
        if dry_run:
            print(f"Would archive manifest {arguments.manifest_id} from build {arguments.build_id}")
            return
        results = galaxy_archiver.archive_manifest(arguments.game_id, arguments.build_id, arguments.manifest_id)
    elif arguments.build_id:
        if dry_run:
            print(f"Would archive build {arguments.build_id} for game {arguments.game_id}")
            # Still download and parse manifests to show what would be archived
            results = galaxy_archiver.archive_build_manifests_only(arguments.game_id, arguments.build_id, platforms)
        elif manifests_only:
            print(f"Archiving manifests for build {arguments.build_id} for game {arguments.game_id}")
            # Download and save build manifests AND their depot manifests, but don't download chunks/blobs
            results = galaxy_archiver.archive_build_and_depot_manifests_only(arguments.game_id, arguments.build_id, platforms)
        else:
            results = galaxy_archiver.archive_build(arguments.game_id, arguments.build_id, platforms, languages)
    else:
        if dry_run:
            print(f"Would archive complete game {arguments.game_id}")
            return
        elif manifests_only:
            print(f"Archiving build manifests for complete game {arguments.game_id}")
            print("Note: Use --build-id for depot manifests. Complete game depot download not yet supported.")
            results = galaxy_archiver.archive_game_manifests(arguments.game_id, platforms)
        else:
            results = galaxy_archiver.archive_game_complete(arguments.game_id, platforms, languages)
    
    print(json.dumps(results, indent=2))


def archive_validate(arguments, unknown_arguments):
    """Handle archive validate subcommand - no authentication required"""
    from pathlib import Path
    
    print("[VALIDATION] ARCHIVE VALIDATION (No Authentication Required)")
    print("=" * 60)
    
    archive_root = Path(arguments.archive_root)
    if not archive_root.exists():
        print(f"❌ Archive directory not found: {archive_root}")
        return
    
    # Summary-only mode
    if arguments.summary_only:
        print(f"📊 Archive Summary:")
        print(f"   📁 Archive root: {archive_root}")
        
        # Count chunks
        chunk_count = 0
        chunk_size = 0
        if (archive_root / "chunks").exists():
            for chunk_dir in (archive_root / "chunks").iterdir():
                if chunk_dir.is_dir():
                    for subdir in chunk_dir.iterdir():
                        if subdir.is_dir():
                            for chunk_file in subdir.iterdir():
                                if chunk_file.is_file():
                                    chunk_count += 1
                                    chunk_size += chunk_file.stat().st_size
        
        # Count blobs
        blob_count = 0
        blob_size = 0
        if (archive_root / "blobs").exists():
            for blob_dir in (archive_root / "blobs").iterdir():
                if blob_dir.is_dir():
                    main_bin = blob_dir / "main.bin"
                    if main_bin.exists():
                        blob_count += 1
                        blob_size += main_bin.stat().st_size
        
        print(f"   📦 V2 Chunks: {chunk_count:,} files ({chunk_size / (1024**3):.2f} GB)")
        print(f"   📦 V1 Blobs: {blob_count} files ({blob_size / (1024**3):.2f} GB)")
        print(f"   📊 Total: {(chunk_size + blob_size) / (1024**3):.2f} GB")
        return
    
    # Use the archiver's comprehensive validation capabilities
    if arguments.app_id and arguments.build_id:
        print(f"🔍 Archive Validation: App {arguments.app_id}, Build {arguments.build_id}")
        
        # Use archiver for comprehensive validation
        galaxy_archiver = archiver.GOGGalaxyArchiver(str(archive_root))
        
        # Run comprehensive validation
        validation_results = galaxy_archiver.validate_archive_comprehensive(
            game_id=arguments.app_id,
            build_id=arguments.build_id,
            platforms=getattr(arguments, 'platforms', ['windows'])
        )
        
        # Display results summary
        if validation_results['validation_summary']['validation_passed'] > 0:
            print(f"\n✅ VALIDATION PASSED")
        else:
            print(f"\n❌ VALIDATION FAILED")
            
        if validation_results['errors']:
            print(f"\n⚠️  Errors encountered:")
            for error in validation_results['errors'][:5]:  # Show first 5 errors
                print(f"   - {error}")
    elif arguments.app_id:
        print(f"🔍 Archive Validation: All builds for App {arguments.app_id}")
        
        # Validate all builds for the game
        galaxy_archiver = archiver.GOGGalaxyArchiver(str(archive_root))
        validation_results = galaxy_archiver.validate_archive_comprehensive(
            game_id=arguments.app_id,
            platforms=getattr(arguments, 'platforms', ['windows'])
        )
        
        # Display results summary
        if validation_results['validation_summary']['validation_passed'] > 0:
            print(f"\n✅ VALIDATION PASSED for {validation_results['validation_summary']['validation_passed']} builds")
        else:
            print(f"\n❌ VALIDATION FAILED")
    else:
        print("❌ Validation requires --app-id (and optionally --build-id)")
        print("   Use --summary-only for general archive statistics")


def archive_list(arguments, unknown_arguments):
    """Handle archive list subcommand - no authentication required"""
    from pathlib import Path
    import json
    
    print("[LISTING] ARCHIVE LISTING (No Authentication Required)")
    print("=" * 50)
    
    archive_root = Path(arguments.archive_root)
    if not archive_root.exists():
        print(f"❌ Archive directory not found: {archive_root}")
        return
    
    # Set up archive structure paths
    blobs_dir = archive_root / "blobs"
    chunks_dir = archive_root / "chunks"
    
    if arguments.builds:
        print(f"📁 Archived Builds:")
        
        # Read from streamlined database
        database_path = archive_root / "metadata" / "archive_database.json"
        builds_found = []
        
        if database_path.exists():
            try:
                import json
                with open(database_path, 'r') as f:
                    data = json.load(f)
                
                builds_data = data.get('builds', [])
                for build in builds_data:
                    # Verify the build file still exists
                    build_path = archive_root / build['archive_path']
                    if build_path.exists():
                        builds_found.append({
                            'type': f"V{build['version']}",
                            'game_id': build['game_id'],
                            'platform': build['platform'],
                            'build_id': build['build_id'],
                            'repository_id': build['repository_id'],
                            'version_name': build.get('version_name', ''),
                            'tags': build.get('tags', []),
                            'path': build['archive_path'],
                            'build_hash': build['build_hash'],
                            'cdn_url': build['cdn_url'],
                            'size': build_path.stat().st_size,
                            'exists': True
                        })
                    else:
                        # Build in database but file missing
                        builds_found.append({
                            'type': f"V{build['version']}",
                            'game_id': build['game_id'],
                            'platform': build['platform'],
                            'build_id': build['build_id'],
                            'repository_id': build['repository_id'],
                            'version_name': build.get('version_name', ''),
                            'tags': build.get('tags', []),
                            'path': build['archive_path'],
                            'build_hash': build['build_hash'],
                            'cdn_url': build['cdn_url'],
                            'size': 0,
                            'exists': False
                        })
                        
            except Exception as e:
                print(f"   ❌ Error reading database: {e}")
                return
        
        if builds_found:
            for build in builds_found:
                status = "✅" if build['exists'] else "❌"
                if arguments.detailed:
                    version_display = f"{build['type']} Build: {build['game_id']}"
                    if build['version_name']:
                        version_display += f" (v{build['version_name']})"
                    print(f"   {status} {version_display}")
                    print(f"      🆔 Build ID: {build['build_id']}")
                    print(f"      🏷️  Repository ID: {build['repository_id']}")
                    print(f"      🌐 Platform: {build['platform']}")
                    if build['tags']:
                        print(f"      🏷️  Tags: {', '.join(build['tags'])}")
                    print(f"      📍 Path: {build['path']}")
                    print(f"      🔗 CDN: {build['cdn_url']}")
                    print(f"      #️⃣  Hash: {build['build_hash'][:16]}...")
                    if build['exists']:
                        print(f"      📏 Size: {build['size']:,} bytes")
                    else:
                        print(f"      ⚠️  File missing from archive")
                    print()
                else:
                    print(f"   {status} {build['type']}: {build['game_id']}/{build['platform']}/{build['build_id']}")
            
            existing_count = sum(1 for b in builds_found if b['exists'])
            missing_count = len(builds_found) - existing_count
            print(f"   📊 Total: {len(builds_found)} builds tracked")
            if missing_count > 0:
                print(f"   ⚠️  Warning: {missing_count} builds missing from archive")
        else:
            print("   📊 No builds found in database")
    
    if arguments.chunks:
        print(f"📦 V2 Chunks:")
        chunk_dirs = list(chunks_dir.iterdir()) if chunks_dir.exists() else []
        chunk_count = 0
        total_size = 0
        
        for chunk_dir in chunk_dirs[:10]:  # Show first 10 directories
            if chunk_dir.is_dir():
                for subdir in chunk_dir.iterdir():
                    if subdir.is_dir():
                        for chunk_file in subdir.iterdir():
                            if chunk_file.is_file():
                                chunk_count += 1
                                total_size += chunk_file.stat().st_size
                                if chunk_count <= 5 or arguments.detailed:  # Show first 5 or all if detailed
                                    print(f"      {chunk_file.name}: {chunk_file.stat().st_size:,} bytes")
        
        print(f"   📊 Total: {chunk_count:,} chunks, {total_size / (1024**3):.2f} GB")
    
    if arguments.blobs:
        print(f"📦 V1 Blobs:")
        if blobs_dir.exists():
            for blob_dir in blobs_dir.iterdir():
                if blob_dir.is_dir():
                    main_bin = blob_dir / "main.bin"
                    if main_bin.exists():
                        size = main_bin.stat().st_size
                        print(f"   📦 {blob_dir.name}/main.bin: {size:,} bytes ({size / (1024**3):.2f} GB)")
        else:
            print("   📊 No blobs found")


def archive_extract(arguments, unknown_arguments):
    """Handle archive extract subcommand - no authentication required"""
    from gogdl.extractor import GOGArchiveExtractor
    import json
    from pathlib import Path
    
    print("[EXTRACT] ARCHIVE EXTRACTION (No Authentication Required)")
    print("=" * 50)
    
    archive_root = Path(arguments.archive_root)
    if not archive_root.exists():
        print(f"❌ Archive directory not found: {archive_root}")
        return
    
    try:
        extractor = GOGArchiveExtractor(str(archive_root), verify_checksums=arguments.verify_checksums)
        
        if arguments.dry_run:
            print("=== DRY RUN MODE - No files will be extracted ===")
        
        print(f"📂 Archive: {archive_root}")
        print(f"📁 Output: {arguments.output_dir}")
        print(f"🎯 Game ID: {arguments.game_id}")
        print(f"🏗️  Build ID: {arguments.build_id}")
        print(f"🖥️  Platform: {arguments.platform}")
        print(f"🌐 Language: {arguments.language}")
        print(f"🔍 Hash Validation: {'Enabled' if arguments.verify_checksums else 'Disabled'}")
        
        # Perform extraction
        results = extractor.extract_build(
            game_id=arguments.game_id,
            build_id=arguments.build_id,
            output_dir=arguments.output_dir,
            platform=arguments.platform
        )
        
        print(f"\n📊 EXTRACTION RESULTS:")
        print(f"   🎮 Build Version: V{results['version']}")
        print(f"   📄 Files Extracted: {results['files_extracted']:,}")
        print(f"   💾 Total Size: {results['total_size']:,} bytes ({results['total_size'] / (1024**3):.2f} GB)")
        
        if results.get('errors'):
            print(f"   ⚠️  Errors: {len(results['errors'])}")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"      • {error}")
            if len(results['errors']) > 5:
                print(f"      ... and {len(results['errors']) - 5} more errors")
        else:
            print(f"   ✅ No errors encountered")
        
        if not arguments.dry_run:
            print(f"\n🎉 Extraction completed successfully!")
            print(f"   📁 Game files are now available in: {arguments.output_dir}")
        else:
            print(f"\n📋 Dry run completed - no files were extracted")
            
    except FileNotFoundError as e:
        print(f"❌ Archive error: {e}")
    except ValueError as e:
        print(f"❌ Build error: {e}")
    except Exception as e:
        print(f"❌ Extraction failed: {e}")


def archive_repair(arguments, unknown_arguments):
    """Handle archive repair subcommand - requires authentication"""
    print("[REPAIR] ARCHIVE REPAIR (Authentication Required)")
    print("=" * 45)
    
    # TODO: Implement repair functionality that:
    # 1. Validates archive using file system truth
    # 2. Identifies corrupted/missing files
    # 3. Re-downloads only the corrupted content
    # 4. Uses the fixed archiver with file system validation
    
    print("Archive repair functionality coming soon...")
    print("Will use file system validation to identify corrupted content")
    print("and re-download only what's actually broken")


def archive_command_dispatcher(arguments, unknown_arguments):
    """Dispatch archive subcommands"""
    if not hasattr(arguments, 'archive_command') or not arguments.archive_command:
        print("ERROR: Archive subcommand required!")
        print("Available subcommands:")
        print("  download  - Download and archive content (requires auth)")
        print("  validate  - Validate archive integrity (no auth)")
        print("  list      - List archived content (no auth)")
        print("  extract   - Extract archived content to playable form (no auth)")
        # print("  repair    - Repair corrupted content (requires auth) [NOT IMPLEMENTED]")
        return
    
    # Route to appropriate handler
    if arguments.archive_command == "download":
        archive_download(arguments, unknown_arguments)
    elif arguments.archive_command == "validate":
        archive_validate(arguments, unknown_arguments)
    elif arguments.archive_command == "list":
        archive_list(arguments, unknown_arguments)
    elif arguments.archive_command == "extract":
        archive_extract(arguments, unknown_arguments)
    # elif arguments.archive_command == "repair":
    #     archive_repair(arguments, unknown_arguments)  # TODO: Not implemented yet
    else:
        print(f"ERROR: Unknown archive subcommand: {arguments.archive_command}")


def archive_game(arguments, unknown_arguments):
    """Legacy archive function - redirect to subcommand dispatcher"""
    return archive_command_dispatcher(arguments, unknown_arguments)


def match_lang(arguments, unknown_arguments):
    lang = languages.Language.parse(arguments.language)
    data = lang.__dict__ if lang else {}
    print(json.dumps(data))

def main():
    arguments, unknown_args = args.init_parser()
    level = logging.INFO
    if '-d' in unknown_args or '--debug' in unknown_args:
        level = logging.DEBUG
    logging.basicConfig(format="[%(name)s] %(levelname)s: %(message)s", level=level)
    logger = logging.getLogger("MAIN")
    logger.debug(arguments)
    if arguments.display_version:
        display_version()
        return
    if not arguments.command:
        print("No command provided!")
        return
    
    # Check for specific commands that don't require authentication
    skip_auth = False
    
    # Specific archive subcommands that don't require auth
    if arguments.command == "archive":
        if hasattr(arguments, 'archive_command') and arguments.archive_command in ["validate", "list"]:
            skip_auth = True
    
    # Other commands that don't require auth  
    if arguments.command in ["import", "lang-match"]:
        skip_auth = True
    
    # Initialize auth unless specifically exempted
    if not skip_auth:
        authorization_manager = auth.AuthorizationManager(arguments.auth_config_path)
        api_handler = api.ApiHandler(authorization_manager)
        clouds_storage_manager = saves.CloudStorageManager(api_handler, authorization_manager)
    else:
        authorization_manager = None
        api_handler = None
        clouds_storage_manager = None

    switcher = {}
    if arguments.command in ["download", "repair", "update", "info"]:
        download_manager = manager.Manager(arguments, unknown_args, api_handler)
        switcher = {
            "download": download_manager.download,
            "repair": download_manager.download,
            "update": download_manager.download,
            "info": download_manager.calculate_download_size,
        }
    elif arguments.command in ["redist", "dependencies"]:
        dependencies_handler = dependencies.DependenciesManager(arguments.ids.split(","), arguments.path, arguments.workers_count, api_handler, print_manifest=arguments.print_manifest)
        if not arguments.print_manifest:
            dependencies_handler.get()
    else:
        switcher = {
            "import": imports.get_info,
            "launch": launch.launch,
            "save-sync": clouds_storage_manager.sync if clouds_storage_manager else None,
            "save-clear": clouds_storage_manager.clear if clouds_storage_manager else None,
            "auth": authorization_manager.handle_cli if authorization_manager else None,
            "lang-match": match_lang,
            "archive": archive_game
        }

    function = switcher.get(arguments.command)
    if function:
        function(arguments, unknown_args)


if __name__ == "__main__":
    freeze_support()
    main()
