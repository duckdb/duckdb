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
    'substrait': ["from_substrait", "get_substrait", "get_substrait_json"]
}

functions = {}
ext_dir = os.path.join('..', '.github', 'config', 'extensions.csv')
reader = csv.reader(open(ext_dir))
# This skips the first row (i.e., the header) of the CSV file.
next(reader)

get_func = "select distinct on(function_name) function_name from duckdb_functions();"
duckdb_path = os.path.join("..",'build', 'release', 'duckdb')
base_functions = os.popen(f'{duckdb_path} -csv -c "{get_func}" ').read().split("\n")[1:-1]

base_functions = {x for x in base_functions}

function_map = {}

# root_dir needs a trailing slash (i.e. /root/dir/)
extension_path = {}
for filename in glob.iglob('/tmp/' + '**/*.duckdb_extension', recursive=True):
    extension_path[os.path.splitext(os.path.basename(filename))[0]] = filename

for extension in reader:
    extension_name = extension[0]
    if extension_name not in extension_path:
        if extension_name not in stored_functions:
            print(f"Missing extension {extension_name}")
            exit(1)
        extension_functions = stored_functions[extension_name]
        print(f"Loading {extension_name} from stored functions: {extension_functions}")
        function_map.update({
            extension_function: extension_name
            for extension_function in (set(extension_functions) - base_functions)
        })
        continue

    print(f"Load {extension_name} at {extension_path[extension_name]}")
    load = f"LOAD '{extension_path[extension_name]}';"
    extension_functions = os.popen(f'{duckdb_path} -unsigned -csv -c "{load}{get_func}" ').read().split("\n")[1:-1]
    function_map.update({
        extension_function: extension_name
        for extension_function in (set(extension_functions) - base_functions)
    })

if args.validate:
    file = open(os.path.join("..","src","include","duckdb", "main", "extension_functions.hpp"),'r')
    pattern = re.compile("{\"(.*?)\", \"(.*?)\"},")
    cur_function_map = dict(pattern.findall(file.read()))
    print("Cur Function Map: ")
    print(sorted(list(cur_function_map)))
    print("Function Map: ")
    print(sorted(list(function_map)))
    if len(cur_function_map) == 0:
        print("Current function map is empty?")
        exit(1)
    if cur_function_map != function_map:
        print("Difference between current functions and function map")
        print(f"Current function map length: {len(cur_function_map)}")
        print(f"Function map length: {len(function_map)}")
        for f in function_map:
            if f not in cur_function_map:
                print(f"Function {f} of function_map does not exist in cur_function_map")
        for f in cur_function_map:
            if f not in function_map:
                print(f"Function {f} of cur_function_map does not exist in function_map")
        exit(1)
else:
    # Generate Header
    file = open(os.path.join("..","src","include","duckdb", "main", "extension_functions.hpp"),'w')
    header = """//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include \"duckdb/common/unordered_map.hpp\"


namespace duckdb { 

struct ExtensionFunction {
    char function[48];
    char extension[48];
};

static constexpr ExtensionFunction EXTENSION_FUNCTIONS[] = { 
"""
    file.write(header)
    # Sort Function Map 
    sorted_function = sorted(function_map)

    for function_name in sorted_function:
        file.write("    {")
        file.write(f'"{function_name}", "{function_map[function_name]}"')
        file.write("}, \n")
    footer = """};
} // namespace duckdb"""
    file.write(footer)

    file.close()
