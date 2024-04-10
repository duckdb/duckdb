import os
import json

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
DUCKDB_INIT_FILE = os.path.join("..", "duckdb", "__init__.py")

START_MARKER = "# START OF CONNECTION WRAPPER"
END_MARKER = "# END OF CONNECTION WRAPPER"

# Read the DUCKDB_INIT_FILE file
with open(DUCKDB_INIT_FILE, 'r') as source_file:
    source_code = source_file.readlines()

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

# Artificial "wrapper" methods on pandas.DataFrames
SPECIAL_METHODS = [
    {'name': 'project', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
    {'name': 'distinct', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
    {'name': 'write_csv', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'None'},
    {'name': 'aggregate', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
    {'name': 'alias', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
    {'name': 'filter', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
    {'name': 'limit', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
    {'name': 'order', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
    {'name': 'query_df', 'args': [{'name': "*args", 'type': 'Any'}], 'return': 'DuckDBPyRelation'},
]

READONLY_PROPERTIES = [
    {'name': 'description', 'return': 'str'},
    {'name': 'rowcount', 'return': 'int'},
]

connection_methods.extend(SPECIAL_METHODS)
connection_methods.extend(READONLY_PROPERTIES)

body = []

SPECIAL_METHOD_NAMES = [x['name'] for x in SPECIAL_METHODS]
READONLY_PROPERTY_NAMES = [x['name'] for x in READONLY_PROPERTIES]


def generate_arguments(name, method) -> str:
    arguments = []
    if name in SPECIAL_METHOD_NAMES:
        # We add 'df' to these methods because they operate on a DataFrame
        arguments.append('df')

    if 'args' in method:
        for arg in method['args']:
            res = arg['name']
            if 'default' in arg:
                res += f" = {arg['default']}"
            arguments.append(res)
    arguments.append('**kwargs')
    return ', '.join(arguments)


def generate_parameters(name, method) -> str:
    if name in READONLY_PROPERTY_NAMES:
        return ''
    arguments = []
    if 'args' in method:
        for arg in method['args']:
            arguments.append(f"{arg['name']}")
    arguments.append('**kwargs')
    result = ', '.join(arguments)
    return '(' + result + ')'


def generate_function_call(name, method) -> str:
    function_call = ''
    if name in SPECIAL_METHOD_NAMES:
        function_call += 'from_df(df).'

    REMAPPED_FUNCTIONS = {'alias': 'set_alias', 'query_df': 'query'}
    if name in REMAPPED_FUNCTIONS:
        function_name = REMAPPED_FUNCTIONS[name]
    else:
        function_name = name
    function_call += function_name
    return function_call


def create_definition(name, method) -> str:
    print(method)
    arguments = generate_arguments(name, method)
    parameters = generate_parameters(name, method)
    function_call = generate_function_call(name, method)

    func = f"""
def {name}({arguments}):
    if 'connection' in kwargs:
        conn =  kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.{function_call}{parameters}
_exported_symbols.append('{name}')
"""
    return func


# We have "duplicate" methods, which are overloaded
written_methods = set()

for method in connection_methods:
    if isinstance(method['name'], list):
        names = method['name']
    else:
        names = [method['name']]

    # Artificially add 'connection' keyword argument
    if 'kwargs' not in method:
        method['kwargs'] = []
    method['kwargs'].append({'name': 'connection', 'type': 'DuckDBPyConnection'})

    for name in names:
        if name in written_methods:
            continue
        if name in ['arrow', 'df']:
            # These methods are ambiguous and are handled in C++ code instead
            continue
        body.append(create_definition(name, method))
        written_methods.add(name)

# ---- End of generation code ----

with_newlines = body
# Recreate the file content by concatenating all the pieces together

new_content = start_section + with_newlines + end_section

print(''.join(with_newlines))

# Write out the modified DUCKDB_INIT_FILE file
with open(DUCKDB_INIT_FILE, 'w') as source_file:
    source_file.write("".join(new_content))
