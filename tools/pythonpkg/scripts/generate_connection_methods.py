import os
import json

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
PYCONNECTION_SOURCE = os.path.join("..", "src", "pyconnection.cpp")

INITIALIZE_METHOD = (
    "static void InitializeConnectionMethods(py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>> &m) {"
)
END_MARKER = "} // END_OF_CONNECTION_METHODS"

# Read the PYCONNECTION_SOURCE file
with open(PYCONNECTION_SOURCE, 'r') as source_file:
    source_code = source_file.readlines()

# Locate the InitializeConnectionMethods function in it
start_index = -1
end_index = -1
for i, line in enumerate(source_code):
    if line.startswith(INITIALIZE_METHOD):
        start_index = i
    elif line.startswith(END_MARKER):
        end_index = i

if start_index == -1 or end_index == -1:
    raise ValueError("Couldn't find start or end marker in source file")

start_section = source_code[: start_index + 1]
end_section = source_code[end_index:]

# Generate the definition code from the json
# Read the JSON file
with open(JSON_PATH, 'r') as json_file:
    connection_methods = json.load(json_file)

regenerated_method = []
regenerated_method.extend(['', ''])

with_newlines = [x + '\n' for x in regenerated_method]
# Recreate the file content by concatenating all the pieces together

new_content = start_section + with_newlines + end_section

# Write out the modified PYCONNECTION_SOURCE file
with open(PYCONNECTION_SOURCE, 'w') as source_file:
    source_file.write("".join(new_content))
