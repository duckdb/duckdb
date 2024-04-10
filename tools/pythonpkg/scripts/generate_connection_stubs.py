import os
import json

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
DUCKDB_STUBS_FILE = os.path.join("..", "duckdb-stubs", "__init__.pyi")

START_MARKER = "    # START OF CONNECTION METHODS"
END_MARKER = "    # END OF CONNECTION METHODS"

# Read the DUCKDB_STUBS_FILE file
with open(DUCKDB_STUBS_FILE, 'r') as source_file:
    source_code = source_file.readlines()

# Locate the InitializeConnectionMethods function in it
start_index = -1
end_index = -1
for i, line in enumerate(source_code):
    if line.startswith(START_MARKER):
        # TODO: handle the case where the start marker appears multiple times
        start_index = i
    elif line.startswith(END_MARKER):
        # TODO: ditto ^
        end_index = i

if start_index == -1 or end_index == -1:
    raise ValueError("Couldn't find start or end marker in source file")

start_section = source_code[: start_index + 1]
end_section = source_code[end_index:]
# ---- Generate the definition code from the json ----

# Read the JSON file
with open(JSON_PATH, 'r') as json_file:
    connection_methods = json.load(json_file)

body = []


def create_arguments(arguments) -> list:
    result = []
    for arg in arguments:
        argument = f"{arg['name']}: {arg['type']}"
        # Add the default argument if present
        if 'default' in arg:
            default = arg['default']
            argument += f" = {default}"
        result.append(argument)
    return result


def create_definition(name, method) -> str:
    print(method)
    definition = f"def {name}(self"
    if 'args' in method:
        definition += ", "
        arguments = create_arguments(method['args'])
        definition += ', '.join(arguments)
    if 'kwargs' in method:
        definition += ", **kwargs"
    definition += ")"
    definition += f" -> {method['return']}: ..."
    return definition


for method in connection_methods:
    if isinstance(method['name'], list):
        names = method['name']
    else:
        names = [method['name']]
    for name in names:
        body.append(create_definition(name, method))

# ---- End of generation code ----

with_newlines = ['\t' + x + '\n' for x in body]
# Recreate the file content by concatenating all the pieces together

new_content = start_section + with_newlines + end_section

print(with_newlines)

exit()

# Write out the modified DUCKDB_STUBS_FILE file
with open(DUCKDB_STUBS_FILE, 'w') as source_file:
    source_file.write("".join(new_content))
