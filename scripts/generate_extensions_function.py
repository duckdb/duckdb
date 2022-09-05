import os
import csv

os.chdir(os.path.dirname(__file__))

functions = {}
reader = csv.reader(open(os.path.join("..",'extension.csv')))
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

file.write("static const unordered_map <std::string,std::string> extension_functions = {") 
for function_name in function_map:
    file.write("{")
    file.write(f"\"{function_name}\", \"{function_map[function_name]}\"") 
    file.write("}, \n")
file.write("}; \n")
file.write("}\n")     

file.close()
