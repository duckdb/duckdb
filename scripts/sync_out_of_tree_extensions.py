#!/usr/bin/env python3
"""
Syncs out-of-tree extensions into extension/external/<name> directories.

Used when DUCKDB_NEW_EXTENSION_BUILD=1. Reads the following environment variables
to determine which extensions to sync:

  BUILD_EXTENSIONS   Semicolon-separated list of extension names
                     (e.g. "spatial;delta;postgres_scanner")
  DUCKDB_EXTENSIONS  Alias for BUILD_EXTENSIONS
  EXTENSION_CONFIGS  Semicolon-separated list of cmake config file paths
                     whose duckdb_extension_load(... GIT_URL ...) calls are parsed

For each out-of-tree extension found, the corresponding cmake config in
.github/config/extensions/<name>.cmake is parsed to obtain GIT_URL, GIT_TAG,
SUBMODULES, and APPLY_PATCHES. The extension is then cloned (or updated) at
extension/external/<name>.
"""

import argparse
import json
import os
import re
import sys
import subprocess
from pathlib import Path


def run_cmd(cmd, cwd=None, check=True):
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"ERROR: command failed: {' '.join(str(c) for c in cmd)}", file=sys.stderr)
        if result.stdout:
            print(result.stdout, file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(1)
    return result


def parse_cmake_file(cmake_path):
    """
    Parse a cmake file and return a list of out-of-tree extension descriptors,
    i.e. duckdb_extension_load() calls that contain GIT_URL.

    Follows include("${EXTENSION_CONFIG_BASE_DIR}/foo.cmake") directives,
    resolving them relative to the directory of cmake_path.

    Each descriptor is a dict with keys:
      name, git_url, git_tag, submodules (list, may be empty), apply_patches (bool)
    """
    cmake_path = Path(cmake_path)
    content = cmake_path.read_text(encoding='utf-8')

    extensions = []

    # Follow include("${EXTENSION_CONFIG_BASE_DIR}/foo.cmake") directives.
    # EXTENSION_CONFIG_BASE_DIR resolves to the 'extensions/' subdirectory next to this file.
    ext_config_base_dir = cmake_path.parent / 'extensions'
    for inc_match in re.finditer(
        r"include\s*\(\s*[\"']?\$\{EXTENSION_CONFIG_BASE_DIR\}/(\S+?\.cmake)[\"']?\s*\)", content
    ):
        included = ext_config_base_dir / inc_match.group(1)
        if included.exists():
            extensions.extend(parse_cmake_file(included))

    # Match duckdb_extension_load( NAME ... ) blocks (possibly multi-line).
    # The closing ) is found by scanning for the first unbalanced ')'.
    for m in re.finditer(r'duckdb_extension_load\s*\(', content):
        start = m.end()
        depth = 1
        i = start
        while i < len(content) and depth > 0:
            if content[i] == '(':
                depth += 1
            elif content[i] == ')':
                depth -= 1
            i += 1
        body = content[start : i - 1]

        # First token is the extension name
        name_match = re.match(r'\s*(\w[\w-]*)', body)
        if not name_match:
            continue
        name = name_match.group(1)

        git_url_match = re.search(r'\bGIT_URL\s+(\S+)', body)
        git_tag_match = re.search(r'\bGIT_TAG\s+(\S+)', body)

        if not git_url_match or not git_tag_match:
            continue  # in-tree extension or DONT_BUILD — skip

        # SUBMODULES can be a space-separated list of paths; everything up to
        # the next keyword or end-of-body
        submodules = []
        submodules_match = re.search(
            r'\bSUBMODULES\s+((?:(?!(?:GIT_URL|GIT_TAG|DONT_LINK|DONT_BUILD|LOAD_TESTS|APPLY_PATCHES|INCLUDE_DIR|TEST_DIR|EXTENSION_VERSION|LINKED_LIBS|\))).)+)',
            body,
        )
        if submodules_match:
            submodules = submodules_match.group(1).split()

        apply_patches = bool(re.search(r'\bAPPLY_PATCHES\b', body))

        extensions.append(
            {
                'name': name,
                'git_url': git_url_match.group(1),
                'git_tag': git_tag_match.group(1),
                'submodules': submodules,
                'apply_patches': apply_patches,
            }
        )

    return extensions


class ExtensionNotCleanError(Exception):
    """Raised when an extension repo has unexpected local state."""

    pass


def resolve_ref(repo_dir, ref):
    """Resolve a git ref to a full commit hash (returns None on failure)."""
    result = run_cmd(['git', 'rev-parse', ref + '^{}'], cwd=repo_dir, check=False)
    if result.returncode == 0:
        return result.stdout.strip()
    result = run_cmd(['git', 'rev-parse', ref], cwd=repo_dir, check=False)
    if result.returncode == 0:
        return result.stdout.strip()
    return None


def get_patch_files(patch_dir):
    """Return sorted list of .patch filenames (not full paths) for an extension."""
    if patch_dir is None or not patch_dir.exists():
        return []
    return sorted(f for f in os.listdir(patch_dir) if f.endswith('.patch'))


def apply_patches_as_commits(ext_dir, patch_dir, patches):
    """
    Apply each patch file with git-apply and create a commit whose message is
    the patch filename (e.g. "fix.patch").
    """
    for patch_name in patches:
        patch_file = patch_dir / patch_name
        run_cmd(['git', 'apply', '--index', str(patch_file)], cwd=ext_dir)
        run_cmd(
            [
                'git',
                '-c',
                'user.name=DuckDB Sync',
                '-c',
                'user.email=sync@duckdb.org',
                'commit',
                '-m',
                patch_name,
                '--no-verify',
            ],
            cwd=ext_dir,
        )


def _exportable_reason(commits_oldest_first):
    """
    Return None if the commit list can be exported as patch files, or a human-readable
    reason string explaining why it cannot.

    Commits are exportable when every message:
      - is a single word (no whitespace)
      - ends in '.patch'
      - is unique
      - appears in strict ascending lexicographic order
    """
    seen = set()
    for i, msg in enumerate(commits_oldest_first):
        if len(msg.split()) != 1:
            return f"commit message '{msg}' contains whitespace (must be a single word)"
        if not msg.endswith('.patch'):
            return f"commit message '{msg}' does not end in '.patch'"
        if msg in seen:
            return f"duplicate commit message '{msg}'"
        seen.add(msg)
        if i > 0 and msg <= commits_oldest_first[i - 1]:
            return f"commits not in ascending lexicographic order: " f"'{commits_oldest_first[i - 1]}' >= '{msg}'"
    return None


def export_commits_as_patches(name, ext_dir, resolved_git_tag, patch_dir):
    """
    Export every commit on top of <resolved_git_tag> as a patch file in <patch_dir>.

    Each commit message becomes the filename; its diff is written as the file content.
    Validates the same constraints as _exportable_reason before writing anything.
    """
    status = run_cmd(['git', 'status', '--porcelain'], cwd=ext_dir)
    if status.stdout.strip():
        raise ExtensionNotCleanError(
            f"Extension '{name}' has uncommitted changes; cannot export patches:\n" f"{status.stdout.rstrip()}"
        )

    log = run_cmd(['git', 'log', '--format=%H %s', f'{resolved_git_tag}..HEAD'], cwd=ext_dir)
    lines = [l.strip() for l in log.stdout.strip().splitlines() if l.strip()]
    if not lines:
        print(f"  {name}: no commits to export")
        return

    # Parse (hash, message) pairs, oldest first
    commits = []
    for line in reversed(lines):
        hash_, _, msg = line.partition(' ')
        commits.append((hash_, msg))

    messages = [msg for _, msg in commits]
    reason = _exportable_reason(messages)
    if reason:
        raise ExtensionNotCleanError(f"Extension '{name}': cannot export patches — {reason}")

    patch_dir.mkdir(parents=True, exist_ok=True)
    for commit_hash, msg in commits:
        patch_content = run_cmd(['git', 'diff', f'{commit_hash}^', commit_hash], cwd=ext_dir).stdout
        (patch_dir / msg).write_text(patch_content, encoding='utf-8')
        print(f"    Exported: {msg}")

    print(f"  {name}: wrote {len(commits)} patch(es) to .github/patches/extensions/{name}/")


def check_extension_clean(name, ext_dir, git_tag, patches):
    """
    Verify the extension repo is in a known-good state:

      1. No uncommitted working-tree changes (git status --porcelain is empty).
      2. The only commits on top of <git_tag> are exactly the patch commits, in
         application order (oldest first).

    Fetches from remote if <git_tag> cannot be resolved locally.

    Returns True  if the repo is already at the correct state (no update needed).
    Returns False if the base is not yet at <git_tag> (update needed but repo is clean).
    Raises ExtensionNotCleanError if either condition fails.
    """
    # Condition 1: working tree clean
    status = run_cmd(['git', 'status', '--porcelain'], cwd=ext_dir)
    if status.stdout.strip():
        raise ExtensionNotCleanError(
            f"Extension '{name}' has uncommitted changes:\n{status.stdout.rstrip()}\n"
            f"To resolve:\n"
            f"* FORCE_APPLY_PATCHES=1           — discard local changes and re-apply patches\n"
            f"* DUCKDB_SKIP_APPLYING_PATCHES=1  — skip patch checks and compile as-is"
        )

    # Resolve git_tag; fetch from remote if it is not known locally yet
    resolved = resolve_ref(ext_dir, git_tag)
    if not resolved:
        run_cmd(['git', 'fetch', '--all'], cwd=ext_dir)
        resolved = resolve_ref(ext_dir, git_tag)
    if not resolved:
        raise ExtensionNotCleanError(f"Extension '{name}': cannot resolve ref '{git_tag}'")

    # Condition 2: commits on top of git_tag are exactly the patch commits
    log = run_cmd(['git', 'log', '--format=%s', f'{resolved}..HEAD'], cwd=ext_dir)
    local_commits_newest_first = [l.strip() for l in log.stdout.strip().splitlines() if l.strip()]
    local_commits_oldest_first = list(reversed(local_commits_newest_first))

    if local_commits_oldest_first != patches:
        export_hint = ''
        if not _exportable_reason(local_commits_oldest_first):
            export_hint = '\n* EXPORT_EXTENSION_PATCHES=1     — export local commits as patch files'
        raise ExtensionNotCleanError(
            f"Extension '{name}' has unexpected local commits.\n"
            f"  Expected (patch commits): {patches}\n"
            f"  Found:                    {local_commits_oldest_first}\n"
            f"To resolve:\n"
            f"* Delete extension/external/{name} and re-run to re-sync\n"
            f"* FORCE_APPLY_PATCHES=1           — discard local changes and re-apply patches\n"
            f"* DUCKDB_SKIP_APPLYING_PATCHES=1  — skip patch checks and compile as-is" + export_hint
        )

    # Check whether the base commit is already the expected git_tag
    n = len(patches)
    base = run_cmd(['git', 'rev-parse', f'HEAD~{n}' if n else 'HEAD'], cwd=ext_dir).stdout.strip()
    return base == resolved


def sync_extension(ext, external_dir, repo_root):
    name = ext['name']
    git_url = ext['git_url']
    git_tag = ext['git_tag']
    submodules = ext['submodules']
    apply_patches_flag = ext['apply_patches']

    ext_dir = external_dir / name
    patch_dir = (repo_root / '.github' / 'patches' / 'extensions' / name) if apply_patches_flag else None
    patches = get_patch_files(patch_dir)
    skip_patches = os.environ.get('DUCKDB_SKIP_APPLYING_PATCHES') == '1'

    if not (ext_dir / '.git').exists():
        # ── Fresh clone ──────────────────────────────────────────────────────
        print(f"  Cloning {name} from {git_url} ...")
        external_dir.mkdir(parents=True, exist_ok=True)
        run_cmd(['git', 'clone', git_url, str(ext_dir)])
        run_cmd(['git', 'checkout', git_tag], cwd=ext_dir)

        if submodules:
            run_cmd(['git', 'submodule', 'update', '--init', '--'] + submodules, cwd=ext_dir)

        if patches and not skip_patches:
            apply_patches_as_commits(ext_dir, patch_dir, patches)

        print(f"  Cloned {name} @ {git_tag}")
    elif skip_patches:
        # ── Skip patches: leave the existing repo untouched ──────────────────
        print(f"  {name}: DUCKDB_SKIP_APPLYING_PATCHES set, skipping patch check and application")
    else:
        # ── Existing repo ────────────────────────────────────────────────────
        force = os.environ.get('FORCE_APPLY_PATCHES') == '1'
        export = os.environ.get('EXPORT_EXTENSION_PATCHES') == '1'

        if export:
            resolved = resolve_ref(ext_dir, git_tag)
            if not resolved:
                run_cmd(['git', 'fetch', '--all'], cwd=ext_dir)
                resolved = resolve_ref(ext_dir, git_tag)
            if not resolved:
                raise ExtensionNotCleanError(f"Extension '{name}': cannot resolve ref '{git_tag}'")
            export_commits_as_patches(
                name, ext_dir, resolved, patch_dir or (repo_root / '.github' / 'patches' / 'extensions' / name)
            )
            return
        elif force:
            print(f"  {name}: force-resetting to {git_tag[:12]} and re-applying patches ...")
            if not resolve_ref(ext_dir, git_tag):
                run_cmd(['git', 'fetch', '--all'], cwd=ext_dir)
            run_cmd(['git', 'reset', '--hard', git_tag], cwd=ext_dir)
            run_cmd(['git', 'clean', '-fd'], cwd=ext_dir)
        else:
            # Raises ExtensionNotCleanError if the repo has unexpected local state.
            already_current = check_extension_clean(name, ext_dir, git_tag, patches)

            if already_current:
                print(f"  {name}: already at {git_tag[:12]}, skipping")
                return

            # Base is not yet at git_tag; strip the current patch commits, move
            # to the new base, and re-apply patches.
            print(f"  Updating {name} to {git_tag} ...")
            n = len(patches)
            if n:
                run_cmd(['git', 'reset', '--hard', f'HEAD~{n}'], cwd=ext_dir)
            run_cmd(['git', 'fetch', '--all'], cwd=ext_dir)
            run_cmd(['git', 'checkout', git_tag], cwd=ext_dir)

        if submodules:
            run_cmd(['git', 'submodule', 'update', '--init', '--'] + submodules, cwd=ext_dir)

        if patches:
            apply_patches_as_commits(ext_dir, patch_dir, patches)

        print(f"  {'Force-reset' if force else 'Updated'} {name} @ {git_tag}")


def collect_extensions(repo_root, build_extensions_arg=None, extension_configs_arg=None):
    """Return a dict of name -> extension descriptor for all out-of-tree extensions to sync."""
    extensions_config_dir = repo_root / '.github' / 'config' / 'extensions'

    raw_build_extensions = (
        build_extensions_arg or os.environ.get('BUILD_EXTENSIONS') or os.environ.get('DUCKDB_EXTENSIONS') or ''
    )
    raw_extension_configs = extension_configs_arg or os.environ.get('EXTENSION_CONFIGS') or ''

    extensions = {}  # name -> descriptor (first seen wins)

    # From named extensions in BUILD_EXTENSIONS / DUCKDB_EXTENSIONS
    for name in re.split(r'[;,\s]+', raw_build_extensions):
        name = name.strip()
        if not name:
            continue
        cmake_path = extensions_config_dir / f'{name}.cmake'
        if not cmake_path.exists():
            continue  # in-tree extension — no cmake file needed
        for ext in parse_cmake_file(cmake_path):
            if ext['name'] == name and ext['name'] not in extensions:
                extensions[ext['name']] = ext

    # From cmake config files in EXTENSION_CONFIGS
    for config_path_str in re.split(r'[;]+', raw_extension_configs):
        config_path_str = config_path_str.strip()
        if not config_path_str:
            continue
        full_path = Path(config_path_str) if os.path.isabs(config_path_str) else repo_root / config_path_str
        if not full_path.exists():
            print(f"  Warning: EXTENSION_CONFIGS path not found: {full_path}", file=sys.stderr)
            continue
        for ext in parse_cmake_file(full_path):
            if ext['name'] not in extensions:
                extensions[ext['name']] = ext

    return extensions


VCPKG_BUILTIN_BASELINE = '84bab45d415d22042bd0b9081aea57f362da3f35'
VCPKG_REGISTRY_BASELINE = 'd485389ad737bb05a5e8afd1fbde5672b559f19e'
VCPKG_REGISTRY_PACKAGES = ['avro-c', 'vcpkg-cmake']


def merge_vcpkg_manifests(synced_extension_names, external_dir, repo_root):
    """
    Collect vcpkg.json files from all synced extensions, merge their dependencies
    and overlay configuration, and write the result to build/vcpkg.json.

    Overlay paths are stored as absolute paths so the manifest is valid regardless
    of which directory cmake is invoked from.
    """
    dependencies_str = []
    dependencies_dict = []
    overlay_ports = []
    overlay_triplets = []
    found_any = False

    openssl_version = os.environ.get('OPENSSL_VERSION_OVERRIDE', '3.6.0')

    for name in synced_extension_names:
        vcpkg_json_path = external_dir / name / 'vcpkg.json'
        if not vcpkg_json_path.exists():
            continue

        found_any = True
        ext_dir = (external_dir / name).resolve()

        with open(vcpkg_json_path, encoding='utf-8') as f:
            data = json.load(f)

        for dep in data.get('dependencies', []):
            if isinstance(dep, str):
                dependencies_str.append(dep)
            elif isinstance(dep, dict):
                dependencies_dict.append(dep)

        config = data.get('vcpkg-configuration', {})
        for rel_path in config.get('overlay-ports', []):
            overlay_ports.append(str((ext_dir / rel_path).resolve()))
        for rel_path in config.get('overlay-triplets', []):
            overlay_triplets.append(str((ext_dir / rel_path).resolve()))

    if not found_any:
        return

    if not os.environ.get('VCPKG_TOOLCHAIN_PATH'):
        extensions_with_vcpkg = [n for n in synced_extension_names if (external_dir / n / 'vcpkg.json').exists()]
        print(
            f"ERROR: the following extensions have vcpkg dependencies but VCPKG_TOOLCHAIN_PATH is not set: "
            f"{', '.join(extensions_with_vcpkg)}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Deduplicate (preserve order, string deps after dict deps)
    seen = set()
    final_deps = []
    for dep in dependencies_dict:
        key = dep['name']
        if key not in seen:
            final_deps.append(dep)
            seen.add(key)
    for dep in dependencies_str:
        if dep not in seen:
            final_deps.append(dep)
            seen.add(dep)

    manifest = {
        'description': "Auto-generated vcpkg.json for combined DuckDB extension build, generated by 'scripts/sync_out_of_tree_extensions.py'",
        'builtin-baseline': VCPKG_BUILTIN_BASELINE,
        'dependencies': final_deps,
        'overrides': [{'name': 'openssl', 'version': openssl_version}],
        'vcpkg-configuration': {
            'registries': [
                {
                    'kind': 'git',
                    'repository': 'https://github.com/duckdb/vcpkg-duckdb-ports',
                    'baseline': VCPKG_REGISTRY_BASELINE,
                    'packages': VCPKG_REGISTRY_PACKAGES,
                }
            ]
        },
    }

    if overlay_ports:
        manifest['vcpkg-configuration']['overlay-ports'] = overlay_ports
    if overlay_triplets:
        manifest['vcpkg-configuration']['overlay-triplets'] = overlay_triplets

    out_path = repo_root / 'build' / 'vcpkg.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=4)
        f.write('\n')

    dep_names = [d if isinstance(d, str) else d['name'] for d in final_deps]
    print(f"  Wrote build/vcpkg.json with dependencies: {dep_names}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--build-extensions', default=None, help='Semicolon-separated list of extension names')
    parser.add_argument('--extension-configs', default=None, help='Semicolon-separated list of cmake config file paths')
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    external_dir = repo_root / 'extension' / 'external'

    extensions = collect_extensions(repo_root, args.build_extensions, args.extension_configs)

    if not extensions:
        print("No out-of-tree extensions to sync.")
        return

    print(f"Syncing out-of-tree extensions into extension/external/: {', '.join(extensions)}")
    for ext in extensions.values():
        try:
            sync_extension(ext, external_dir, repo_root)
        except ExtensionNotCleanError as e:
            print(f"\nERROR: {e}", file=sys.stderr)
            sys.exit(1)

    merge_vcpkg_manifests(list(extensions.keys()), external_dir, repo_root)

    print("Sync complete.")


if __name__ == '__main__':
    main()
