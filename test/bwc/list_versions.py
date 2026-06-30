#!/usr/bin/env python3
import argparse
import json
from os.path import abspath, dirname
import sys

sys.path.insert(0, dirname(abspath(__file__)))
from utils.version_list import list_supported_duckdb_version_groups, list_supported_duckdb_versions


def main():
    parser = argparse.ArgumentParser(description="List supported DuckDB versions for BWC")
    parser.add_argument("--json", action="store_true", help="Output JSON array")
    parser.add_argument("--groups-json", action="store_true", help="Output JSON array grouped by vX.Y series")
    args = parser.parse_args()

    if args.groups_json:
        print(json.dumps(list_supported_duckdb_version_groups()))
        return

    versions = list_supported_duckdb_versions()
    if args.json:
        print(json.dumps(versions))
    else:
        for version in versions:
            print(version)


if __name__ == "__main__":
    main()
