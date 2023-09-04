import os
import csv
import re
import argparse
import glob

os.chdir(os.path.dirname(__file__))

parser = argparse.ArgumentParser(description='Generates/Validates extension_functions.hpp file')

parser.add_argument(
    '--validate',
    action=argparse.BooleanOptionalAction,
    help='If set  will validate that extension_entries.hpp is up to date, otherwise it generates the extension_functions.hpp file.',
)


args = parser.parse_args()

stored_functions = {
    'substrait': ["from_substrait", "get_substrait", "get_substrait_json", "from_substrait_json"],
    'arrow': ["scan_arrow_ipc", "to_arrow_ipc"],
    'spatial': [],
}
stored_settings = {'substrait': [], 'arrow': [], 'spatial': []}

functions = {}


# Parses the extension config files for which extension names there are to be expected
def parse_extension_txt():
    extensions_file = os.path.join("..", "build", "extension_configuration", "extensions.txt")
    if not os.path.isfile(extensions_file):
        print(
            "please run `make extension_configuration` with the desired extension configuration before running the script"
        )
        exit(1)
    with open(extensions_file) as f:
        return [line.rstrip() for line in f]


extension_names = parse_extension_txt()

# Add exception for jemalloc as it doesn't produce a loadable extension but is in the config
if "jemalloc" in extension_names:
    extension_names.remove("jemalloc")

ext_hpp = os.path.join("..", "src", "include", "duckdb", "main", "extension_entries.hpp")
get_functions_query = "select distinct function_name from duckdb_functions();"
get_settings_query = "select distinct name from duckdb_settings();"
duckdb_path = os.path.join("..", 'build', 'release', 'duckdb')


def get_query(sql_query, load_query):
    return os.popen(f'{duckdb_path} -csv -unsigned -c "{load_query}{sql_query}" ').read().split("\n")[1:-1]


def get_functions(load=""):
    return set(get_query(get_functions_query, load))


def get_settings(load=""):
    return set(get_query(get_settings_query, load))


base_functions = get_functions()
base_settings = get_settings()

function_map = {}
settings_map = {}

# root_dir needs a trailing slash (i.e. /root/dir/)
extension_path = {}
for filename in glob.iglob('/tmp/' + '**/*.duckdb_extension', recursive=True):
    extension_path[os.path.splitext(os.path.basename(filename))[0]] = filename


# Update global maps with settings/functions from `extension_name`
def update_extensions(extension_name, function_list, settings_list):
    global function_map, settings_map
    function_map.update(
        {
            extension_function.lower(): extension_name.lower()
            for extension_function in (set(function_list) - base_functions)
        }
    )
    settings_map.update(
        {
            extension_setting.lower(): extension_name.lower()
            for extension_setting in (set(settings_list) - base_settings)
        }
    )


# Get all extension entries from DuckDB's catalog
for extension_name in extension_names:
    if extension_name not in extension_path:
        if extension_name not in stored_functions or extension_name not in stored_settings:
            print(f"Missing extension {extension_name} and not found in stored_functions/stored_settings")
            exit(1)
        extension_functions = stored_functions[extension_name]
        extension_settings = stored_settings[extension_name]
        print(f"Loading {extension_name} from stored functions: {extension_functions}")
        update_extensions(extension_name, extension_functions, extension_settings)
        continue

    print(f"Load {extension_name} at {extension_path[extension_name]}")
    load = f"LOAD '{extension_path[extension_name]}';"
    extension_functions = get_functions(load)
    extension_settings = get_settings(load)
    update_extensions(extension_name, extension_functions, extension_settings)


# Get the slice of the file containing the var (assumes // END_OF_<varname> comment after var)
def get_slice_of_file(var_name, file_str):
    begin = file_str.find(var_name)
    end = file_str.find("END_OF_" + var_name)
    return file_str[begin:end]


# Parses the extension_entries.hpp file
def parse_extension_entries(file_path):
    file = open(file_path, 'r')
    pattern = re.compile("{\"(.*?)\", \"(.*?)\"}[,}\n]")
    file_blob = file.read()

    # Get the extension functions
    ext_functions_file_blob = get_slice_of_file("EXTENSION_FUNCTIONS", file_blob)
    cur_function_map = dict(pattern.findall(ext_functions_file_blob))

    # Get the extension settings
    ext_settings_file_blob = get_slice_of_file("EXTENSION_SETTINGS", file_blob)
    cur_settings_map = dict(pattern.findall(ext_settings_file_blob))

    # Get the extension types
    ext_copy_functions_blob = get_slice_of_file("EXTENSION_COPY_FUNCTIONS", file_blob)
    cur_copy_functions_map = dict(pattern.findall(ext_copy_functions_blob))

    # Get the extension types
    ext_types_file_blob = get_slice_of_file("EXTENSION_TYPES", file_blob)
    cur_types_map = dict(pattern.findall(ext_types_file_blob))

    return {
        'functions': cur_function_map,
        'settings': cur_settings_map,
        'types': cur_types_map,
        'copy_functions': cur_copy_functions_map,
    }


def print_map_diff(d1, d2):
    s1 = set(d1.items())
    s2 = set(d2.items())
    diff = str(s1 ^ s2)
    print("Diff between maps: " + diff + "\n")


if args.validate:
    parsed_entries = parse_extension_entries(ext_hpp)
    if function_map != parsed_entries['functions']:
        print("Function map mismatches:")
        print("Found in " + str(duckdb_path) + ": " + str(sorted(function_map)) + "\n")
        print("Parsed from extension_entries.hpp: " + str(parsed_entries['functions']) + "\n")
        print_map_diff(function_map, parsed_entries['functions'])
        exit(1)
    if settings_map != parsed_entries['settings']:
        print("Settings map mismatches:")
        print("Found: " + str(settings_map) + "\n")
        print("Parsed from extension_entries.hpp: " + str(parsed_entries['settings']) + "\n")
        print_map_diff(settings_map, parsed_entries['settings'])
        exit(1)

    print("All entries found: ")
    print(" > functions: " + str(len(parsed_entries['functions'])))
    print(" > settings:  " + str(len(parsed_entries['settings'])))
else:
    # extension_functions
    file = open(ext_hpp, 'w')
    header = """//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_entries.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include \"duckdb/common/unordered_map.hpp\"

// NOTE: this file is generated by scripts/generate_extensions_function.py. Check out the check-load-install-extensions 
//       job in .github/workflows/LinuxRelease.yml on how to use it 

namespace duckdb { 

struct ExtensionEntry {
    char name[48];
    char extension[48];
};

static constexpr ExtensionEntry EXTENSION_FUNCTIONS[] = {
"""
    file.write(header)
    # functions
    sorted_function = sorted(function_map)

    for function_name in sorted_function:
        file.write("    {")
        file.write(f'"{function_name}", "{function_map[function_name]}"')
        file.write("}, \n")
    file.write("}; // END_OF_EXTENSION_FUNCTIONS\n")

    # settings
    header = """
static constexpr ExtensionEntry EXTENSION_SETTINGS[] = {
"""
    file.write(header)
    # Sort Function Map
    sorted_settings = sorted(settings_map)

    for settings_name in sorted_settings:
        file.write("    {")
        file.write(f'"{settings_name.lower()}", "{settings_map[settings_name]}"')
        file.write("}, \n")
    footer = """}; // END_OF_EXTENSION_SETTINGS
    
// Note: these are currently hardcoded in scripts/generate_extensions_function.py
// TODO: automate by passing though to script via duckdb 
static constexpr ExtensionEntry EXTENSION_COPY_FUNCTIONS[] = {
    {"parquet", "parquet"}, 
    {"json", "json"}
}; // END_OF_EXTENSION_COPY_FUNCTIONS

// Note: these are currently hardcoded in scripts/generate_extensions_function.py
// TODO: automate by passing though to script via duckdb 
static constexpr ExtensionEntry EXTENSION_TYPES[] = {
    {"json", "json"}, 
    {"inet", "inet"}, 
    {"geometry", "spatial"}
}; // END_OF_EXTENSION_TYPES

// Note: these are currently hardcoded in scripts/generate_extensions_function.py
// TODO: automate by passing though to script via duckdb
static constexpr ExtensionEntry EXTENSION_FILE_PREFIXES[] = {
    {"http://", "httpfs"},
    {"https://", "httpfs"},
    {"s3://", "httpfs"},
//    {"azure://", "azure"}
}; // END_OF_EXTENSION_FILE_PREFIXES

// Note: these are currently hardcoded in scripts/generate_extensions_function.py
// TODO: automate by passing though to script via duckdb
static constexpr ExtensionEntry EXTENSION_FILE_POSTFIXES[] = {
    {".parquet", "parquet"},
    {".json", "json"},
    {".jsonl", "json"},
    {".ndjson", "json"}
}; // END_OF_EXTENSION_FILE_POSTFIXES

// Note: these are currently hardcoded in scripts/generate_extensions_function.py
// TODO: automate by passing though to script via duckdb
static constexpr ExtensionEntry EXTENSION_FILE_CONTAINS[] = {
    {".parquet?", "parquet"},
    {".json?", "json"},
    {".ndjson?", ".jsonl?"},
    {".jsonl?", ".ndjson?"}
}; // EXTENSION_FILE_CONTAINS

static constexpr const char *AUTOLOADABLE_EXTENSIONS[] = {
//    "azure",
    "autocomplete",
    "excel",
    "fts",
    "httpfs",
    // "inet", 
    // "icu",
    "json",
    "parquet",
    "sqlsmith",
    "tpcds",
    "tpch",
    "visualizer"
}; // END_OF_AUTOLOADABLE_EXTENSIONS

} // namespace duckdb"""
    file.write(footer)

    file.close()
