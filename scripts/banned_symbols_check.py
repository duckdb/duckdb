import subprocess
import sys
from pathlib import Path
import argparse


banned_symbols = [
    "std::basic_ofstream",
    "std::basic_ifstream",
    "std::basic_fstream",
]
parser = argparse.ArgumentParser(description="Check object files for banned symbols")
parser.add_argument(
    "--directory", type=Path, action='store', help="Directory to search for .o files", default='build/release/src'
)
args = parser.parse_args()

if not args.directory.is_dir():
    sys.exit(f"Error: '{args.directory}' is not a directory")


def check_object_file(path: Path) -> list[tuple[str, str]]:
    global banned_symbols
    """Run nm -C on an object file and return any banned symbols found."""
    try:
        result = subprocess.run(
            ["nm", "-C", str(path)],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        sys.exit("Error: 'nm' command not found")

    violations = []
    for line in result.stdout.splitlines():
        for banned in banned_symbols:
            if banned in line:
                violations.append((banned, line.strip()))
    return violations


all_violations = {}

for obj_file in args.directory.rglob("*.o"):
    print(obj_file)
    violations = check_object_file(obj_file)
    if violations:
        all_violations[obj_file] = violations

if all_violations:
    print("ERROR: Banned symbols found:\n")
    for path, violations in all_violations.items():
        print(f"{path}:")
        for symbol, line in violations:
            print(f"  [{symbol}] {line}")
        print()
    sys.exit(1)

print("No banned symbols found.")
