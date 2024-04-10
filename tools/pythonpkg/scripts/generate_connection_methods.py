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
# ---- Generate the definition code from the json ----

# Read the JSON file
with open(JSON_PATH, 'r') as json_file:
    connection_methods = json.load(json_file)

body = []
body.extend(['', ''])

DEFAULT_ARGUMENT_MAP = {'True': 'true', 'False': 'false', 'None': 'py::none()'}


def map_default(val):
    if val in DEFAULT_ARGUMENT_MAP:
        return DEFAULT_ARGUMENT_MAP[val]
    return val


for conn in connection_methods:
    if isinstance(conn['name'], list):
        names = conn['name']
    else:
        names = [conn['name']]
    for name in names:
        definition = f"m.def(\"{name}\""
        definition += ", "
        definition += f"""&DuckDBPyConnection::{conn['function']}"""
        definition += ", "
        definition += f"\"{conn['docs']}\""
        if 'args' in conn:
            definition += ", "
            arguments = []
            for arg in conn['args']:
                argument = f"py::arg(\"{arg['name']}\")"
                # TODO: add '.none(false)' if required (add 'allow_none' to the JSON)
                # Add the default argument if present
                if 'default' in arg:
                    default = map_default(arg['default'])
                    argument += f" = {default}"
                arguments.append(argument)
            definition += ', '.join(arguments)
        if 'kwargs' in conn:
            definition += ", "
            definition += "py::kw_only(), "
            keyword_arguments = []
            for kwarg in conn['kwargs']:
                keyword_argument = f"py::arg(\"{kwarg['name']}\")"
                # TODO: add '.none(false)' if required (add 'allow_none' to the JSON)
                # Add the default argument if present
                if 'default' in arg:
                    default = map_default(arg['default'])
                    keyword_argument += f" = {default}"
                keyword_arguments.append(keyword_argument)
            definition += ', '.join(keyword_arguments)
        definition += ");"
        body.append(definition)

# ---- End of generation code ----

with_newlines = [x + '\n' for x in body]
print(''.join(with_newlines))
exit()
# Recreate the file content by concatenating all the pieces together

new_content = start_section + with_newlines + end_section

# Write out the modified PYCONNECTION_SOURCE file
with open(PYCONNECTION_SOURCE, 'w') as source_file:
    source_file.write("".join(new_content))
