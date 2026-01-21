import subprocess
import re
import sys
import json
import argparse


def run_and_parse_tests(generate_json=False):
    test_command = ["./build/release/test/unittest", "--test-config", "test/configs/peg_parser_strict.json"]

    print("--- Starting Test Process ---")

    # Run tests and capture combined output
    result = subprocess.run(test_command, capture_output=True, text=True)
    output_data = result.stdout + "\n" + result.stderr

    # Pattern 1: Dashed Headers (Sqllogictests)
    # Looks for:
    # ----------
    # test/path/file.test
    # ----------
    header_pattern = re.compile(r"-{10,}\s*\n(test/.*?\.test(?:_slow)?)\s*\n-{10,}", re.MULTILINE)

    # Pattern 2: Numbered Failure List (Standard Unittests)
    # Looks for: "1. test/path/file.test:11" at the start of a line
    list_pattern = re.compile(r"^\d+\.\s+(test/.*?\.test(?:_slow)?)(?::\d+)?", re.MULTILINE)

    # Extract matches from both patterns
    failed_headers = header_pattern.findall(output_data)
    failed_list = list_pattern.findall(output_data)

    # Deduplicate and sort the final list
    unique_failed_files = sorted(list(set(failed_headers + failed_list)))

    if not unique_failed_files:
        print("\n✅ All tests passed!")
        return

    if generate_json:
        # Generate the specific JSON format for peg_parser_strict.json
        json_output = {
            "reason": f"Transformer rule not implemented (Total count: {len(unique_failed_files)})",
            "paths": unique_failed_files,
        }

        output_filename = "generated_failures.json"
        with open(output_filename, 'w') as f:
            json.dump(json_output, f, indent=2)

        print(f"\n📁 File generated: {output_filename}")
        print(json.dumps(json_output, indent=2))
    else:
        print("\n--- Validated Failing Test Files ---")
        for file in unique_failed_files:
            print(f"  - {file}")

        print("\n" + "=" * 40)
        print(f"Unique Failing Files: {len(unique_failed_files)}")
        print("=" * 40)

    sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckDB PEG Test Wrapper")
    parser.add_argument("-g", "--generate", action="store_true", help="Generate a JSON file with failing test paths")

    args = parser.parse_args()
    run_and_parse_tests(generate_json=args.generate)
