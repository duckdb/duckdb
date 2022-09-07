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
for row in reader:
    extension_name = row[0]
    print("Install/Load " + extension_name)
    install = f"INSTALL {extension_name};"
    load = f"LOAD {extension_name};"
    extension_functions = os.popen(f'{duckdb_path} -csv -c "{install}{load}{get_func}" ').read().split("\n")[1:-1]
    extension_functions = {x for x in extension_functions}
    extension_functions = extension_functions.difference(base_functions)
    for extension_function in extension_functions:
        function_map[extension_function] = extension_name

if args.validate:
    cur_function_map = {}
    file = open(os.path.join("..","src","include","extension_functions.hpp"),'r')
    pattern = re.compile("{\"(.*?)\", \"(.*?)\"},")
    for line in file:
        if pattern.match(line):
            split_line = line.split("\"")
            cur_function_map[split_line[1]] = split_line[3]
    assert cur_function_map == function_map
else:
    # Generate Header
    file = open(os.path.join("..","src","include","extension_functions.hpp"),'w')
    file.write("//===----------------------------------------------------------------------===//\n")
    file.write("//                         DuckDB\n")
    file.write("//\n")
    file.write("// extension_functions.hpp\n")
    file.write("//\n")
    file.write("//\n")
    file.write("//===----------------------------------------------------------------------===//\n")
    file.write("\n")
    file.write("#pragma once\n")
    file.write("\n")
    file.write("#include \"duckdb/common/unordered_map.hpp\"")
    file.write("\n\n")
    file.write("namespace duckdb { \n\n")
    file.write("struct ExtensionFunction { \n")
    file.write("char extension[48]; \n")
    file.write("char function[48]; \n")
    file.write("}; \n")
    file.write("static constexpr ExtensionFunction EXTENSION_FUNCTIONS[] = {\n")

    for function_name, function_ext in function_map.items():
        file.write("{")
        file.write(f'"{function_name}", "function_ext}"')
        file.write("}, \n")
    file.write("}; \n")
    file.write("}\n")

    file.close()
