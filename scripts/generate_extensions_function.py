import os
import csv
import re
import argparse

os.chdir(os.path.dirname(__file__))

parser = argparse.ArgumentParser(description='Generates/Validates extension_functions.hpp file')

parser.add_argument('--validate', action=argparse.BooleanOptionalAction,
                    help='If set  will validate that extension_functions.hpp is up to date, otherwise it generates the extension_functions.hpp file.')


args = parser.parse_args()

functions = {}
reader = csv.reader(open(os.path.join("..",'extensions.csv')))
# This skips the first row (i.e., the header) of the CSV file.
next(reader)

get_func = "select distinct on(function_name) function_name from duckdb_functions();"
duckdb_path = os.path.join("..",'build', 'release', 'duckdb')
base_functions = os.popen(f'{duckdb_path} -csv -c "{get_func}" ').read().split("\n")[1:-1]

base_functions = {x for x in base_functions}

function_map = {}
for extension in reader:
    extension_name = extension[0]
    print("Load " + extension_name)
    load = f"LOAD '/tmp/{extension_name}';"
    extension_functions = os.popen(f'{duckdb_path} -csv -c "{load}{get_func}" ').read().split("\n")[1:-1]
    function_map.update({
        extension_function: extension_name
        for extension_function in (set(extension_functions) - base_functions)
    })

if args.validate:
    cur_function_map = {}
    file = open(os.path.join("..","src","include","extension_functions.hpp"),'r')
    pattern = re.compile("{\"(.*?)\", \"(.*?)\"},")
    for line in file:
        if pattern.match(line):
            split_line = line.split("\"")
            cur_function_map[split_line[1]] = split_line[3]
    print("Cur Function Map: ")
    print(cur_function_map)
    print("Function Map: ")
    print(function_map)
    assert cur_function_map == function_map
else:
    # Generate Header
    file = open(os.path.join("..","src","include","extension_functions.hpp"),'w')
    header = """//===----------------------------------------------------------------------===//
//                         DuckDB
//
// extension_functions.hpp
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
