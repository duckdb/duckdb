#!/usr/bin/env python3

import argparse
import io
import os
import platform
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_ASSET_BASE_URL = "https://duckdb-staging.duckdb.org"
DOWNLOAD_USER_AGENT = "duckdb-ci/check-staged-extensions"
DOWNLOAD_RETRIES = 2
DOWNLOAD_RETRY_SECONDS = 10
EXTENSION_SEPARATOR_TRANSLATION = str.maketrans({",": " ", ";": " "})


def normalize_arch(machine: str) -> str:
    machine = machine.lower()
    if machine in {"x86_64", "amd64"}:
        return "amd64"
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    return machine


def cli_asset_name(system: str | None = None, machine: str | None = None) -> str:
    system = (system or platform.system()).lower()
    arch = normalize_arch(machine or platform.machine())

    if system == "linux" and arch in {"amd64", "arm64"}:
        return f"duckdb-cli-linux-{arch}.tar.gz"
    if system == "darwin" and arch in {"amd64", "arm64"}:
        return f"duckdb-cli-osx-{arch}.tar.gz"

    raise RuntimeError(f"unsupported staged CLI platform: {system}/{arch}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test staged extension install/load with staged CLI.")
    parser.add_argument("--duckdb-commit", required=True, help="Full DuckDB commit used for the staged build.")
    parser.add_argument("--duckdb-version", default="", help="Optional staged DuckDB version directory.")
    parser.add_argument("--asset-base-url", default=DEFAULT_ASSET_BASE_URL)
    parser.add_argument(
        "--extensions",
        default="",
        help="Optional comma, semicolon or whitespace-separated extension allowlist.",
    )
    return parser.parse_args()


def cli_asset_url(asset_base_url: str, duckdb_commit: str, duckdb_version: str) -> str:
    short_sha = duckdb_commit[:10]
    base = asset_base_url.rstrip("/")
    asset_name = cli_asset_name()
    if duckdb_version:
        return f"{base}/{short_sha}/{duckdb_version}/duckdb/duckdb/github_release/{asset_name}"
    return f"{base}/{short_sha}/duckdb/duckdb/github_release/{asset_name}"


def extract_cli(archive_bytes: bytes, target: Path) -> None:
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
        members = archive.getmembers()
        if len(members) != 1 or members[0].name != "duckdb" or not members[0].isfile():
            raise ValueError("staged CLI archive must contain only a top-level duckdb file")

        source = archive.extractfile(members[0])
        if source is None:
            raise ValueError("failed to read duckdb from staged CLI archive")
        with source, target.open("wb") as destination:
            destination.write(source.read())

    target.chmod(0o755)


def download_cli(url: str, target: Path) -> None:
    last_error: Exception | None = None
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            print(f"Downloading staged CLI ({attempt}/{DOWNLOAD_RETRIES}): {url}", flush=True)
            request = urllib.request.Request(url, headers={"User-Agent": DOWNLOAD_USER_AGENT})
            with urllib.request.urlopen(request, timeout=60) as response:
                archive_bytes = response.read()
            extract_cli(archive_bytes, target)
            return
        except (OSError, ValueError, tarfile.TarError, urllib.error.URLError, urllib.error.HTTPError) as error:
            last_error = error
            if attempt == DOWNLOAD_RETRIES:
                break
            print(f"Staged CLI is not ready yet: {error}", flush=True)
            time.sleep(DOWNLOAD_RETRY_SECONDS)
    raise RuntimeError(f"failed to download staged CLI after {DOWNLOAD_RETRIES} attempts: {last_error}")


def run_duckdb(
    duckdb: Path, sql: str, home: Path, extension_directory: Path, *, csv: bool = False
) -> subprocess.CompletedProcess[str]:
    command = [str(duckdb)]
    if csv:
        command.extend(["-csv", "-noheader"])
    command.extend(["-c", f"SET extension_directory='{sql_string(extension_directory)}'; {sql}"])

    env = os.environ.copy()
    env["HOME"] = str(home)
    env["XDG_CACHE_HOME"] = str(home / ".cache")

    return subprocess.run(command, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)


def print_completed_process(result: subprocess.CompletedProcess[str]) -> None:
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr)


def sql_string(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def path_safe_name(name: str) -> str:
    return "".join(character if character.isalnum() or character in "._-" else "_" for character in name)


def query_extensions(duckdb: Path, home: Path, extension_directory: Path) -> list[str]:
    result = run_duckdb(
        duckdb,
        "SELECT extension_name FROM duckdb_extensions() ORDER BY extension_name;",
        home,
        extension_directory,
        csv=True,
    )
    print_completed_process(result)
    if result.returncode != 0:
        raise RuntimeError("failed to query duckdb_extensions() from staged CLI")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def parse_extensions(extensions: str) -> list[str]:
    return [extension for extension in extensions.translate(EXTENSION_SEPARATOR_TRANSLATION).split() if extension]


def filter_extensions(available_extensions: list[str], requested_extensions: list[str]) -> list[str]:
    if not requested_extensions:
        return available_extensions

    available = set(available_extensions)
    missing_extensions = [extension for extension in requested_extensions if extension not in available]
    if missing_extensions:
        raise RuntimeError(f"requested extensions not reported by duckdb_extensions(): {', '.join(missing_extensions)}")
    return requested_extensions


def print_cli_context(duckdb: Path, home: Path, extension_directory: Path) -> None:
    result = run_duckdb(duckdb, "PRAGMA version; PRAGMA platform;", home, extension_directory)
    print_completed_process(result)
    if result.returncode != 0:
        raise RuntimeError("staged CLI failed to report version/platform")


def check_extension(duckdb: Path, extension: str, root: Path) -> tuple[bool, bool]:
    extension_path_name = path_safe_name(extension)
    home = root / "home" / extension_path_name
    extension_directory = root / "extensions" / extension_path_name
    home.mkdir(parents=True)
    extension_directory.mkdir(parents=True)

    install = run_duckdb(duckdb, f"INSTALL {sql_identifier(extension)};", home, extension_directory)
    print_completed_process(install)
    install_ok = install.returncode == 0
    if not install_ok:
        print(f"::error title=Extension install failed::{extension}")

    load = run_duckdb(duckdb, f"LOAD {sql_identifier(extension)};", home, extension_directory)
    print_completed_process(load)
    load_ok = load.returncode == 0
    if not load_ok:
        print(f"::error title=Extension load failed::{extension}")

    return install_ok, load_ok


def main() -> int:
    args = parse_args()
    url = cli_asset_url(args.asset_base_url, args.duckdb_commit, args.duckdb_version)

    with tempfile.TemporaryDirectory(prefix="duckdb-staged-extensions-") as temp_dir:
        root = Path(temp_dir)
        duckdb = root / "duckdb"
        download_cli(url, duckdb)

        shared_home = root / "home" / "metadata"
        shared_extension_directory = root / "extensions" / "metadata"
        shared_home.mkdir(parents=True)
        shared_extension_directory.mkdir(parents=True)

        print_cli_context(duckdb, shared_home, shared_extension_directory)
        extensions = query_extensions(duckdb, shared_home, shared_extension_directory)
        if not extensions:
            raise RuntimeError("duckdb_extensions() returned no extensions")
        extensions = filter_extensions(extensions, parse_extensions(args.extensions))

        failed: list[str] = []
        for extension in extensions:
            print(f"::group::Extension {extension}")
            try:
                install_ok, load_ok = check_extension(duckdb, extension, root)
                if not install_ok or not load_ok:
                    failed.append(extension)
            except Exception as error:
                failed.append(extension)
                print(f"::error title=Extension check failed::{extension}: {error}")
            finally:
                print("::endgroup::")

        if failed:
            print(f"Failed extensions: {', '.join(failed)}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"::error title=Staged extension smoke test failed::{error}", file=sys.stderr)
        sys.exit(1)
