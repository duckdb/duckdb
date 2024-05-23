import os
import json
from get_cpp_connection_methods import get_methods, FunctionParam, ConnectionMethod

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
DUCKDB_PYTHON_SOURCE = os.path.join("..", "duckdb_python.cpp")

START_MARKER = "\t// START_OF_CONNECTION_METHODS"
END_MARKER = "\t// END_OF_CONNECTION_METHODS"

LAMBDA_FORMAT = """
	    []({param_definitions}) {{
		    if (!conn) {{
			    conn = DuckDBPyConnection::DefaultConnection();
		    }}
		    {opt_return}conn->{function_name}({parameter_names});
	    }}"""


def generate():
    # Read the DUCKDB_PYTHON_SOURCE file
    with open(DUCKDB_PYTHON_SOURCE, 'r') as source_file:
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

    # Read the JSON file
    with open(JSON_PATH, 'r') as json_file:
        connection_methods = json.load(json_file)

    # Collect the definitions from the pyconnection.hpp header

    method_definitions = get_methods()

    DEFAULT_ARGUMENT_MAP = {
        'True': 'true',
        'False': 'false',
        'None': 'py::none()',
        'PythonUDFType.NATIVE': 'PythonUDFType::NATIVE',
        'PythonExceptionHandling.DEFAULT': 'PythonExceptionHandling::FORWARD_ERROR',
        'FunctionNullHandling.DEFAULT': 'FunctionNullHandling::DEFAULT_NULL_HANDLING',
    }

    def map_default(val):
        if val in DEFAULT_ARGUMENT_MAP:
            return DEFAULT_ARGUMENT_MAP[val]
        return val

    def create_arguments(arguments) -> list:
        result = []
        for arg in arguments:
            argument = f"py::arg(\"{arg['name']}\")"
            if 'allow_none' in arg:
                value = str(arg['allow_none']).lower()
                argument += f".none({value})"
            # Add the default argument if present
            if 'default' in arg:
                default = map_default(arg['default'])
                argument += f" = {default}"
            result.append(argument)
        return result

    def get_lambda_definition(definition: ConnectionMethod) -> str:
        param_definitions = [x.proto for x in definition.params]
        param_definitions.append('shared_ptr<DuckDBPyConnection> conn = nullptr')
        param_definitions = ", ".join(param_definitions)

        param_names = [x.name for x in definition.params]
        param_names = ", ".join(param_names)

        format_dict = {
            'param_definitions': param_definitions,
            'opt_return': '' if definition.is_void else 'return ',
            'function_name': definition.name,
            'parameter_names': param_names,
        }
        return LAMBDA_FORMAT.format_map(format_dict)

    def create_definition(name, method, lambda_def) -> str:
        definition = f"m.def(\"{name}\""
        definition += ", "
        definition += lambda_def
        definition += ", "
        definition += f"\"{method['docs']}\""
        if 'args' in method:
            definition += ", "
            arguments = create_arguments(method['args'])
            definition += ', '.join(arguments)
        if 'kwargs' in method:
            definition += ", "
            definition += "py::kw_only(), "
            arguments = create_arguments(method['kwargs'])
            definition += ', '.join(arguments)
        definition += ");"
        return definition

    body = []
    for method in connection_methods:
        if isinstance(method['name'], list):
            names = method['name']
        else:
            names = [method['name']]
        for name in names:
            function_name = method['function']
            lambda_def = get_lambda_definition(method_definitions[function_name])
            if 'kwargs' not in method:
                method['kwargs'] = []
            method['kwargs'].append({'name': 'conn', 'type': 'Optional[DuckDBPyConnection]', 'default': 'None'})
            body.append(create_definition(name, method, lambda_def))

    # ---- End of generation code ----

    with_newlines = ['\t' + x + '\n' for x in body]
    # Recreate the file content by concatenating all the pieces together

    new_content = start_section + with_newlines + end_section

    # Write out the modified DUCKDB_PYTHON_SOURCE file
    with open(DUCKDB_PYTHON_SOURCE, 'w') as source_file:
        source_file.write("".join(new_content))


if __name__ == '__main__':
    # raise ValueError("Please use 'generate_connection_code.py' instead of running the individual script(s)")
    generate()
