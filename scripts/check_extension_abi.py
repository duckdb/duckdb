#!/usr/bin/env python3
"""Check that the generated extension vtable stays loadable by older engines.

An extension copies `sizeof(its own struct)` bytes out of the struct the engine
hands it (`duckdb_ext_api = *res` in duckdb_extension.h). That is only sound if
the extension's struct is a *prefix* of the engine's. So for every released
DuckDB version, a build pinned to that version must lay out exactly the slots
that version promised, in the same order, and nothing more.

This compares a pinned preprocessor run of the generated header against the
vtable each release tag actually shipped. It needs the release tags present
locally; without them it reports which are missing and exits 0, so a shallow
clone does not fail the build for the wrong reason.

Usage: python3 scripts/check_extension_abi.py [--cc cc]
"""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HEADER_DIR = "src/include"
ENGINE_HEADER = "src/include/duckdb/main/capi/extension_api.hpp"

CLIENT_HEADER = "src/include/duckdb.h"

RELEASES = ["v1.2.0", "v1.3.0", "v1.4.0", "v1.5.0", "v1.5.2"]


def _slots_from_struct(text: str) -> list[str]:
    end = text.index("} duckdb_ext_api_v1;")
    start = text.rindex("typedef struct {", 0, end)
    body = text[start:end]
    out = []
    for chunk in body.split(";"):
        found = re.findall(r"\(\*(\w+)\)", chunk)
        if found:
            out.append(found[0])
    return out


def pinned_slots(cc: str, version: str) -> list[str]:
    """Slots a build pinned to `version` compiles, in order."""
    major, minor, patch = version.lstrip("v").split(".")
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "probe.c"
        src.write_text('#include "duckdb_extension.h"\n')
        result = subprocess.run(
            [
                cc,
                "-E",
                "-I",
                HEADER_DIR,
                f"-DDUCKDB_EXTENSION_API_VERSION_MAJOR={major}",
                f"-DDUCKDB_EXTENSION_API_VERSION_MINOR={minor}",
                f"-DDUCKDB_EXTENSION_API_VERSION_PATCH={patch}",
                str(src),
            ],
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        raise SystemExit(f"pinning {version} failed to preprocess:\n{result.stderr}")
    return _slots_from_struct(result.stdout)


def renames() -> dict[str, str]:
    """Old name -> new name, read off the generated header's compat section.

    A rename keeps the slot and the signature, so the two spellings are the same
    ABI. Mapping them before comparing keeps a rename from reading as though
    every later slot had shifted. Reading it from the header rather than a
    hand-kept list means a future rename needs no change here.
    """
    text = Path(CLIENT_HEADER).read_text()
    marker = "// Renamed constructs"
    if marker not in text:
        return {}
    section = text[text.index(marker) :]
    return dict(re.findall(r"^#define\s+(\w+)\s+(\w+)$", section, re.MULTILINE))


def release_slots(tag: str, aliases: dict[str, str]) -> list[str] | None:
    """Slots the engine shipped at `tag`, or None when the tag is unavailable."""
    result = subprocess.run(["git", "show", f"{tag}:{ENGINE_HEADER}"], capture_output=True, text=True)
    if result.returncode != 0:
        return None
    return [aliases.get(n, n) for n in re.findall(r"result\.(duckdb_\w+)\s*=", result.stdout)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cc", default="cc")
    args = parser.parse_args()

    aliases = renames()
    if aliases:
        print(
            f"treating {len(aliases)} rename(s) as ABI-compatible: "
            + ", ".join(f"{o}->{n}" for o, n in sorted(aliases.items()))
        )
    shipped = {tag: release_slots(tag, aliases) for tag in RELEASES}
    missing = [tag for tag, slots in shipped.items() if slots is None]
    if missing:
        print(f"skipping: release tags not available locally: {', '.join(missing)}")
        return 0

    failures = []
    for pin in RELEASES:
        got = pinned_slots(args.cc, pin)
        # Every engine that would accept this extension must start with these slots.
        for tag in RELEASES:
            engine = shipped[tag]
            if len(engine) < len(got):
                continue  # too old to load this pin; the version check rejects it
            if engine[: len(got)] != got:
                bad = next(i for i, (a, b) in enumerate(zip(got, engine)) if a != b)
                failures.append(
                    f"pin {pin} ({len(got)} slots) is not a prefix of {tag}: "
                    f"slot {bad} is '{got[bad]}' but {tag} has '{engine[bad]}'"
                )
        print(f"pin {pin}: {len(got)} slots, prefix-compatible")

    if failures:
        print("\nABI COMPATIBILITY BROKEN:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1
    print("\nall pinned builds are valid prefixes of every engine that accepts them")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
