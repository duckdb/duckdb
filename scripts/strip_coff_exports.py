#!/usr/bin/env python3
"""Remove COFF DLL export directives; preserve archives containing LLVM bitcode."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


EXPORT = re.compile(rb'(?i)(?<!\S)[/-]export:(?:"[^"]*"|[^\s"]+)(?:,[^\s]*)?')
LLVM_BITCODE_MAGIC = (b"BC\xc0\xde", b"\xde\xc0\x17\x0b")


def filter_directives(directives):
    return EXPORT.subn(lambda match: b" " * len(match.group()), directives)


def run(*command, **options):
    return subprocess.run(command, check=True, capture_output=True, **options)


def strip_exports(path, objcopy="llvm-objcopy", ar="llvm-ar", librarian="llvm-lib", jobs=4):
    path = Path(path).resolve()
    names = run(ar, "t", str(path), text=True).stdout.splitlines()
    if not names or len(set(names)) != len(names):
        raise ValueError("Expected a nonempty archive with unique member names")

    with tempfile.TemporaryDirectory(prefix="duckdb-exports-", dir=path.parent) as directory:
        work = Path(directory)
        members = work / "members"
        members.mkdir()
        basenames = [os.path.normcase(Path(name).name) for name in names]
        if any(count > 1 for count in Counter(basenames).values()):
            # Extract by full member name to avoid overwriting equal basenames.
            objects = []
            for index, name in enumerate(names):
                obj = members / f"member-{index:06d}.obj"
                obj.write_bytes(run(ar, "pP", str(path), name).stdout)
                objects.append(obj)
        else:
            run(ar, "x", str(path), cwd=members)
            objects = [members / Path(name).name for name in names]
            if not all(obj.is_file() for obj in objects):
                raise ValueError("Unexpected archive extraction layout")

        # Bitcode has no COFF sections. Check all members first so mixed archives stay intact too.
        for obj in objects:
            with obj.open("rb") as member:
                if member.read(4) in LLVM_BITCODE_MAGIC:
                    print(f"Keeping DLL exports in {path}: archive contains LLVM bitcode (IPO/LTO)", file=sys.stderr)
                    return 0

        def filter_object(obj):
            directives_file = obj.with_name(obj.name + ".directives")
            dumped = subprocess.run(
                [objcopy, f"--dump-section=.drectve={directives_file}", str(obj)],
                capture_output=True,
                text=True,
            )
            if dumped.returncode:
                if "section '.drectve' not found" in dumped.stderr:
                    return 0
                dumped.check_returncode()
            filtered, count = filter_directives(directives_file.read_bytes())
            if count:
                directives_file.write_bytes(filtered)
                run(objcopy, f"--update-section=.drectve={directives_file}", str(obj))
            return count

        with ThreadPoolExecutor(max_workers=jobs) as workers:
            removed = sum(workers.map(filter_object, objects))

        # Response files avoid Windows command-line limits for non-unity builds.
        response = work / "objects.rsp"
        response.write_text("\n".join(subprocess.list2cmdline([str(obj)]) for obj in objects), encoding="utf-8")
        staged = work / "filtered.lib"
        run(librarian, f"/out:{staged}", f"@{response}")
        os.replace(staged, path)
    return removed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--objcopy", default="llvm-objcopy")
    parser.add_argument("--ar", default="llvm-ar")
    parser.add_argument("--lib", dest="librarian", default="llvm-lib")
    parser.add_argument("--jobs", type=int, default=4)
    arguments = parser.parse_args()
    if arguments.jobs < 1:
        parser.error("--jobs must be positive")
    try:
        removed = strip_exports(arguments.archive, arguments.objcopy, arguments.ar, arguments.librarian, arguments.jobs)
    except subprocess.CalledProcessError as error:
        message = error.stderr
        if isinstance(message, bytes):
            message = message.decode(errors="replace")
        parser.exit(1, f"{message or error}\n")
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    print(f"Removed {removed} DLL export directives from {arguments.archive}")


if __name__ == "__main__":
    main()
