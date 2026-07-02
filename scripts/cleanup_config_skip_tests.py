#!/usr/bin/env python3
"""
Script to clean up JSON configuration files by removing skip_tests entries
that are already inherited from parent configurations.
"""

import json
import os
import sys
import argparse
from pathlib import Path


def load_config(filepath):
    """Load a JSON configuration file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def save_config(filepath, config):
    """Save a JSON configuration file with proper formatting."""
    with open(filepath, 'w') as f:
        json.dump(config, f, indent=2)
        f.write('\n')


def get_inherited_skip_tests(config_path, base_dir, visited=None):
    """
    Recursively collect all inherited skip test paths.
    Returns a set of test paths.
    """
    if visited is None:
        visited = set()

    # Prevent infinite loops from circular inheritance
    config_path = os.path.abspath(config_path)
    if config_path in visited:
        return set()
    visited.add(config_path)

    config = load_config(config_path)
    inherited_paths = set()

    # Check if this config inherits from another
    if 'inherit_skip_tests' in config:
        inherit_path = config['inherit_skip_tests']
        # Resolve relative path from base directory (repo root)
        full_inherit_path = os.path.join(base_dir, inherit_path)

        if os.path.exists(full_inherit_path):
            # First, recursively get any tests inherited by the parent
            inherited_paths.update(get_inherited_skip_tests(full_inherit_path, base_dir, visited))

            # Then add the parent's own skip tests
            parent_config = load_config(full_inherit_path)
            if 'skip_tests' in parent_config:
                for skip_group in parent_config['skip_tests']:
                    if 'paths' in skip_group:
                        inherited_paths.update(skip_group['paths'])
        else:
            print(f"Warning: Inherited config not found: {full_inherit_path}", file=sys.stderr)

    return inherited_paths


def cleanup_config(config_path, base_dir, dry_run=False):
    """
    Clean up a config file by removing skip_tests that are already inherited.
    Returns True if changes were made, False otherwise.
    """
    config = load_config(config_path)

    # If no skip_tests or no inheritance, nothing to clean
    if 'skip_tests' not in config or 'inherit_skip_tests' not in config:
        return False

    inherited_tests = get_inherited_skip_tests(config_path, base_dir)

    if not inherited_tests:
        return False

    changes_made = False
    removed_tests = []

    # Process each skip_tests group
    new_skip_tests = []
    for skip_group in config['skip_tests']:
        if 'paths' not in skip_group:
            new_skip_tests.append(skip_group)
            continue

        # Filter out paths that are inherited
        original_paths = skip_group['paths']
        filtered_paths = [p for p in original_paths if p not in inherited_tests]

        # Track what was removed
        removed_from_group = [p for p in original_paths if p in inherited_tests]
        if removed_from_group:
            removed_tests.extend(removed_from_group)
            changes_made = True

        # Only keep the group if it still has paths
        if filtered_paths:
            new_group = skip_group.copy()
            new_group['paths'] = filtered_paths
            new_skip_tests.append(new_group)

    if changes_made:
        print(f"\n{config_path}:")
        print(f"  Removed {len(removed_tests)} inherited test(s):")
        for test in removed_tests:
            print(f"    - {test}")

        if not dry_run:
            config['skip_tests'] = new_skip_tests
            # Remove skip_tests entirely if empty
            if not new_skip_tests:
                del config['skip_tests']
            save_config(config_path, config)
            print("  Changes saved.")
        else:
            print("  (dry run - no changes saved)")

    return changes_made


def find_config_files(directory):
    """Find all JSON config files in the given directory."""
    config_files = []
    for filepath in Path(directory).rglob('*.json'):
        config_files.append(str(filepath))
    return config_files


def main():
    parser = argparse.ArgumentParser(description='Clean up JSON config files by removing inherited skip_tests.')
    parser.add_argument(
        'configs',
        nargs='*',
        help='Config files to clean up. If none specified, processes all JSON files in test/configs/',
    )
    parser.add_argument(
        '--base-dir',
        default=None,
        help='Base directory for resolving inherit_skip_tests paths (default: current directory)',
    )
    parser.add_argument(
        '--dry-run', '-n', action='store_true', help='Show what would be removed without making changes'
    )
    parser.add_argument('--verbose', '-v', action='store_true', help='Show more details')

    args = parser.parse_args()

    # Determine base directory
    base_dir = args.base_dir or os.getcwd()

    # Find config files to process
    if args.configs:
        config_files = args.configs
    else:
        # Default to test/configs directory
        config_dir = os.path.join(base_dir, 'test', 'configs')
        if os.path.exists(config_dir):
            config_files = find_config_files(config_dir)
        else:
            print(f"Error: Config directory not found: {config_dir}", file=sys.stderr)
            sys.exit(1)

    if not config_files:
        print("No config files found to process.")
        return

    if args.verbose:
        print(f"Processing {len(config_files)} config file(s)...")
        if args.dry_run:
            print("(dry run mode)")

    total_changed = 0
    for config_path in sorted(config_files):
        try:
            if cleanup_config(config_path, base_dir, args.dry_run):
                total_changed += 1
        except json.JSONDecodeError as e:
            print(f"Error parsing {config_path}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Error processing {config_path}: {e}", file=sys.stderr)

    if total_changed == 0:
        print("\nNo redundant inherited tests found.")
    else:
        print(f"\n{'Would clean' if args.dry_run else 'Cleaned'} {total_changed} file(s).")


if __name__ == '__main__':
    main()
