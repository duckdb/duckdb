# This script is used by CI to modify the deployment matrix for the extension distribution

import argparse
import json
import sys
import logging

# Define command-line arguments
parser = argparse.ArgumentParser(description="Filter a JSON file based on excluded duckdb_arch values and select an OS")
parser.add_argument("--input", required=True, help="Input JSON file path")
parser.add_argument("--exclude", required=True, help="Semicolon-separated list of excluded duckdb_arch values")
parser.add_argument("--output", help="Output JSON file path")
parser.add_argument("--pretty", action="store_true", help="Pretty print the output JSON")
parser.add_argument("--select_os", help="Select an OS to include in the output JSON")
parser.add_argument("--deploy_matrix", action="store_true", help="Create a merged list used in deploy step")
args = parser.parse_args()

# Parse the input file path, excluded arch values, and output file path
input_json_file_path = args.input
excluded_arch_values = args.exclude.split(";")
output_json_file_path = args.output
select_os = args.select_os

# Read the input JSON file
with open(input_json_file_path, "r") as json_file:
    data = json.load(json_file)


# Function to filter entries based on duckdb_arch values
def filter_entries(data, arch_values):
    for os, config in data.items():
        if "include" in config:
            config["include"] = [entry for entry in config["include"] if entry["duckdb_arch"] not in arch_values]
        if not config["include"]:
            del config["include"]

    return data


# Filter the JSON data
filtered_data = filter_entries(data, excluded_arch_values)

# Select an OS if specified
if select_os:
    found = False
    for os in filtered_data.keys():
        if os == select_os:
            filtered_data = filtered_data[os]
            found = True
            break
    if found == False:
        logging.warning('A selection OS was provided but not found')
        filtered_data = []

# When deploy_matrix is specified, we only output a single merged include list with all the duckdb_archs
elif args.deploy_matrix:
    deploy_archs = []

    for os, config in filtered_data.items():
        if "include" in config:
            for item in config["include"]:
                deploy_archs.append({"duckdb_arch": item["duckdb_arch"]})

    filtered_data = {"include": deploy_archs}

# Determine the JSON formatting
indent = 2 if args.pretty else None

# If no output file is provided, print to stdout
if output_json_file_path:
    with open(output_json_file_path, "w") as output_json_file:
        if filtered_data:
            json.dump(filtered_data, output_json_file, indent=indent)
else:
    json.dump(filtered_data, sys.stdout, indent=indent)
