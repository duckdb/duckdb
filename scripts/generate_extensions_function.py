import os
import csv
import re
import argparse
import glob

os.chdir(os.path.dirname(__file__))

parser = argparse.ArgumentParser(description='Generates/Validates extension_functions.hpp file')

parser.add_argument('--validate', action=argparse.BooleanOptionalAction,
                    help='If set  will validate that extension_functions.hpp is up to date, otherwise it generates the extension_functions.hpp file.')


args = parser.parse_args()

stored_functions = {
    'substrait': ["from_substrait", "get_substrait", "get_substrait_json", "from_substrait_json"],
    'arrow': ["scan_arrow_ipc", "to_arrow_ipc"]
}
stored_settings = {
    'substrait': [],
    'arrow': []
}

functions = {}
ext_dir = os.path.join('..', '.github', 'config', 'extensions.csv')
ext_hpp = os.path.join("..", "src","include","duckdb", "main", "extension_entries.hpp")
reader = csv.reader(open(ext_dir))
# This skips the first row (i.e., the header) of the CSV file.
next(reader)

get_functions_query = "select distinct function_name from duckdb_functions();"
get_settings_query = "select distinct name from duckdb_settings();"
duckdb_path = os.path.join("..",'build', 'release', 'duckdb')

def get_query(sql_query, load_query):
    return os.popen(f'{duckdb_path} -csv -unsigned -c "{load_query}{sql_query}" ').read().split("\n")[1:-1]

def get_functions(load = ""):
    return set(get_query(get_functions_query, load))

def get_settings(load = ""):
    return set(get_query(get_settings_query, load))

base_functions = get_functions()
base_settings = get_settings()

function_map = {}
settings_map = {}

# root_dir needs a trailing slash (i.e. /root/dir/)
extension_path = {}
for filename in glob.iglob('/tmp/' + '**/*.duckdb_extension', recursive=True):
    extension_path[os.path.splitext(os.path.basename(filename))[0]] = filename

def update_extensions(extension_name, function_list, settings_list):
    global function_map, settings_map
    function_map.update({
        extension_function.lower(): extension_name.lower()
        for extension_function in (set(function_list) - base_functions)
    })
    settings_map.update({
        extension_setting.lower(): extension_name.lower()
        for extension_setting in (set(settings_list) - base_settings)
    })


for extension in reader:
    extension_name = extension[0]
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

if args.validate:
    file = open(ext_hpp,'r')
    pattern = re.compile("{\"(.*?)\", \"(.*?)\"},")
    cur_function_map = dict(pattern.findall(file.read()))
    function_map.update(settings_map)
    print("Cur Function + Settings Map: ")
    print(sorted(list(cur_function_map)))
    print("Function + Settings Map: ")
    print(sorted(list(function_map)))
    if len(cur_function_map) == 0:
        print("Current function map is empty?")
        exit(1)
    if cur_function_map != function_map:
        print("Difference between current functions and function map")
        print(f"Current function map length: {len(cur_function_map)}")
        print(f"Function map length: {len(function_map)}")
        for f in function_map:
            if f.lower() not in cur_function_map:
                print(f"Function {f} of function_map does not exist in cur_function_map")
        for f in cur_function_map:
            if f.lower() not in function_map:
                print(f"Function {f} of cur_function_map does not exist in function_map")
        exit(1)
else:
    # extension_functions
    file = open(ext_hpp,'w')
    header = """//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_entries.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include \"duckdb/common/unordered_map.hpp\"


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
    file.write("};\n")

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
    footer = """};
} // namespace duckdb"""
    file.write(footer)

    file.close()
