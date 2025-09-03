import os
import json
import re
import glob
import copy
from packaging.version import Version
from functools import reduce
from pathlib import Path


EXT_API_DEFINITION_PATTERN = "src/include/duckdb/main/capi/header_generation/apis/v1/*/*.json"

# The JSON files that define all available CAPI functions
CAPI_FUNCTION_DEFINITION_FILES = 'src/include/duckdb/main/capi/header_generation/functions/**/*.json'


# The original order of the function groups in the duckdb.h files. We maintain this for easier PR reviews.
# TODO: replace this with alphabetical ordering in a separate PR
ORIGINAL_FUNCTION_GROUP_ORDER = [
    'open_connect',
    'configuration',
    'query_execution',
    'result_functions',
    'safe_fetch_functions',
    'helpers',
    'date_time_timestamp_helpers',
    'hugeint_helpers',
    'unsigned_hugeint_helpers',
    'decimal_helpers',
    'prepared_statements',
    'bind_values_to_prepared_statements',
    'execute_prepared_statements',
    'extract_statements',
    'pending_result_interface',
    'value_interface',
    'logical_type_interface',
    'data_chunk_interface',
    'vector_interface',
    'validity_mask_functions',
    'scalar_functions',
    'aggregate_functions',
    'table_functions',
    'table_function_bind',
    'table_function_init',
    'table_function',
    'replacement_scans',
    'profiling_info',
    'appender',
    'table_description',
    'arrow_interface',
    'threading_information',
    'streaming_result_interface',
    'cast_functions',
    'expression_interface',
]


def get_extension_api_version(ext_api_definitions):
    latest_version = ""

    for version_entry in ext_api_definitions:
        if version_entry["version"].startswith("v"):
            latest_version = version_entry["version"]
        if version_entry["version"].startswith("unstable_"):
            break

    return latest_version


# Parse the CAPI_FUNCTION_DEFINITION_FILES to get the full list of functions
def parse_capi_function_definitions(function_definition_file_pattern):
    # Collect all functions
    # function_files = glob.glob(CAPI_FUNCTION_DEFINITION_FILES, recursive=True)
    function_files = glob.glob(function_definition_file_pattern, recursive=True)

    function_groups = []
    function_map = {}

    # Read functions
    for file in function_files:
        with open(file, "r") as f:
            try:
                json_data = json.loads(f.read())
            except json.decoder.JSONDecodeError as err:
                print(f"Invalid JSON found in {file}: {err}")
                exit(1)

            function_groups.append(json_data)
            for function in json_data["entries"]:
                if function["name"] in function_map:
                    print(f"Duplicate symbol found when parsing C API file {file}: {function['name']}")
                    exit(1)

                function["group"] = json_data["group"]
                if "deprecated" in json_data:
                    function["group_deprecated"] = json_data["deprecated"]

                function_map[function["name"]] = function

    # Reorder to match original order: purely intended to keep the PR review sane
    function_groups_ordered = []

    if len(function_groups) != len(ORIGINAL_FUNCTION_GROUP_ORDER):
        print(
            "The list used to match the original order of function groups in the original the duckdb.h file does not match the new one. Did you add a new function group? please also add it to ORIGINAL_FUNCTION_GROUP_ORDER for now."
        )

    for order_group in ORIGINAL_FUNCTION_GROUP_ORDER:
        curr_group = next(group for group in function_groups if group["group"] == order_group)
        function_groups.remove(curr_group)
        function_groups_ordered.append(curr_group)

    return (function_groups_ordered, function_map)


# Read extension API
def parse_ext_api_definitions(ext_api_definition):
    api_definitions = {}
    versions = []
    dev_versions = []
    for file in list(glob.glob(ext_api_definition)):
        with open(file, "r") as f:
            try:
                obj = json.loads(f.read())
                api_definitions[obj["version"]] = obj
                if obj["version"].startswith("unstable_"):
                    dev_versions.append(obj["version"])
                else:
                    if Path(file).stem != obj["version"]:
                        print(
                            f"\nMismatch between filename and version in file for {file}. Note that unstable versions should have a version starting with 'unstable_' and that stable versions should have the version as their filename"
                        )
                        exit(1)
                    versions.append(obj["version"])

            except json.decoder.JSONDecodeError as err:
                print(f"\nInvalid JSON found in {file}: {err}")
                exit(1)

    versions.sort(key=Version)
    dev_versions.sort()

    return [api_definitions[x] for x in (versions + dev_versions)]
