import os
import json

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
WRAPPER_JSON_PATH = os.path.join("connection_wrapper_methods.json")
DUCKDB_STUBS_FILE = os.path.join("..", "duckdb-stubs", "__init__.pyi")

START_MARKER = "# START OF CONNECTION WRAPPER"
END_MARKER = "# END OF CONNECTION WRAPPER"


def generate():
    # Read the DUCKDB_STUBS_FILE file
    with open(DUCKDB_STUBS_FILE, 'r') as source_file:
        source_code = source_file.readlines()

    start_index = -1
    end_index = -1
    for i, line in enumerate(source_code):
        if line.startswith(START_MARKER):
            if start_index != -1:
                raise ValueError("Encountered the START_MARKER a second time, quitting!")
            start_index = i
        elif line.startswith(END_MARKER):
            if end_index != -1:
                raise ValueError("Encountered the END_MARKER a second time, quitting!")
            end_index = i

    if start_index == -1 or end_index == -1:
        raise ValueError("Couldn't find start or end marker in source file")

    start_section = source_code[: start_index + 1]
    end_section = source_code[end_index:]
    # ---- Generate the definition code from the json ----

    methods = []

    # Read the JSON file
    with open(JSON_PATH, 'r') as json_file:
        connection_methods = json.load(json_file)

    with open(WRAPPER_JSON_PATH, 'r') as json_file:
        wrapper_methods = json.load(json_file)

    methods.extend(connection_methods)
    methods.extend(wrapper_methods)

    # On DuckDBPyConnection these are read_only_properties, they're basically functions without requiring () to invoke
    # that's not possible on 'duckdb' so it becomes a function call with no arguments (i.e duckdb.description())
    READONLY_PROPERTY_NAMES = ['description', 'rowcount']

    # These methods are not directly DuckDBPyConnection methods,
    # they first call 'from_df' and then call a method on the created DuckDBPyRelation
    SPECIAL_METHOD_NAMES = [x['name'] for x in wrapper_methods if x['name'] not in READONLY_PROPERTY_NAMES]

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
        definition = f"def {name}("
        arguments = []
        if name in SPECIAL_METHOD_NAMES:
            arguments.append('df: pandas.DataFrame')
        if 'args' in method:
            arguments.extend(create_arguments(method['args']))
        if 'kwargs' in method:
            if not any(x.startswith('*') for x in arguments):
                arguments.append("*")
            arguments.extend(create_arguments(method['kwargs']))
        definition += ', '.join(arguments)
        definition += ")"
        definition += f" -> {method['return']}: ..."
        return definition

    # We have "duplicate" methods, which are overloaded
    # maybe we should add @overload to these instead, but this is easier
    written_methods = set()

    body = []
    for method in methods:
        if isinstance(method['name'], list):
            names = method['name']
        else:
            names = [method['name']]

        # Artificially add 'connection' keyword argument
        if 'kwargs' not in method:
            method['kwargs'] = []
        method['kwargs'].append({'name': 'connection', 'type': 'DuckDBPyConnection', 'default': '...'})

        for name in names:
            if name in written_methods:
                continue
            body.append(create_definition(name, method))
            written_methods.add(name)

    # ---- End of generation code ----

    with_newlines = [x + '\n' for x in body]
    # Recreate the file content by concatenating all the pieces together

    new_content = start_section + with_newlines + end_section

    # Write out the modified DUCKDB_STUBS_FILE file
    with open(DUCKDB_STUBS_FILE, 'w') as source_file:
        source_file.write("".join(new_content))


if __name__ == '__main__':
    raise ValueError("Please use 'generate_connection_code.py' instead of running the individual script(s)")
    # generate()
