import os
import sys
import json

# Requires `python3 -m pip install cxxheaderparser pcpp`
from get_cpp_methods import get_methods, FunctionParam, ConnectionMethod
from typing import List, Tuple

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
DUCKDB_PYTHON_SOURCE = os.path.join("..", "duckdb_python.cpp")

START_MARKER = "\t// START_OF_CONNECTION_METHODS"
END_MARKER = "\t// END_OF_CONNECTION_METHODS"

LAMBDA_FORMAT = """
        []({param_definitions}) {{
            {opt_retrieval}
            if (!conn) {{
                conn = DuckDBPyConnection::DefaultConnection();
            }}
            {opt_return}conn->{function_name}({parameter_names});
        }}"""

PY_INIT_FORMAT = """
from .duckdb import (
{item_list}
)

_exported_symbols.extend([
{str_item_list}
])
"""

WRAPPER_JSON_PATH = os.path.join("connection_wrapper_methods.json")

DUCKDB_INIT_FILE = os.path.join("..", "duckdb", "__init__.py")
INIT_PY_START = "# START OF CONNECTION WRAPPER"
INIT_PY_END = "# END OF CONNECTION WRAPPER"

# Read the JSON file
with open(WRAPPER_JSON_PATH, 'r') as json_file:
    wrapper_methods = json.load(json_file)

# On DuckDBPyConnection these are read_only_properties, they're basically functions without requiring () to invoke
# that's not possible on 'duckdb' so it becomes a function call with no arguments (i.e duckdb.description())
READONLY_PROPERTY_NAMES = ['description', 'rowcount']

# These methods are not directly DuckDBPyConnection methods,
# they first call 'FromDF' and then call a method on the created DuckDBPyRelation
SPECIAL_METHOD_NAMES = [x['name'] for x in wrapper_methods if x['name'] not in READONLY_PROPERTY_NAMES]

RETRIEVE_CONN_FROM_DICT = """auto connection_arg = kwargs.contains("conn") ? kwargs["conn"] : py::none();
    auto conn = py::cast<shared_ptr<DuckDBPyConnection>>(connection_arg);
"""


def is_py_args(method):
    if 'args' not in method:
        return False
    args = method['args']
    if len(args) == 0:
        return False
    if args[0]['name'] != '*args':
        return False
    return True


def is_py_kwargs(method):
    return 'kwargs_as_dict' in method and method['kwargs_as_dict'] == True


def remove_section(content, start_marker, end_marker) -> Tuple[List[str], List[str]]:
    start_index = -1
    end_index = -1
    for i, line in enumerate(content):
        if line.startswith(start_marker):
            if start_index != -1:
                raise ValueError("Encountered the START_MARKER a second time, quitting!")
            start_index = i
        elif line.startswith(end_marker):
            if end_index != -1:
                raise ValueError("Encountered the END_MARKER a second time, quitting!")
            end_index = i

    if start_index == -1 or end_index == -1:
        raise ValueError("Couldn't find start or end marker in source file")

    start_section = content[: start_index + 1]
    end_section = content[end_index:]
    return (start_section, end_section)


def generate():
    # Read the DUCKDB_PYTHON_SOURCE file
    with open(DUCKDB_PYTHON_SOURCE, 'r') as source_file:
        source_code = source_file.readlines()
    start_section, end_section = remove_section(source_code, START_MARKER, END_MARKER)

    # Read the DUCKDB_INIT_FILE file
    with open(DUCKDB_INIT_FILE, 'r') as source_file:
        source_code = source_file.readlines()
    py_start, py_end = remove_section(source_code, INIT_PY_START, INIT_PY_END)

    # ---- Generate the definition code from the json ----

    # Read the JSON file
    with open(JSON_PATH, 'r') as json_file:
        connection_methods = json.load(json_file)

    # Collect the definitions from the pyconnection.hpp header

    cpp_connection_defs = get_methods('DuckDBPyConnection')
    cpp_relation_defs = get_methods('DuckDBPyRelation')

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
            if arg['name'] == '*args':
                # py::args() should not have a corresponding py::arg(<name>)
                continue
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

    def get_lambda_definition(name, method, definition: ConnectionMethod) -> str:
        param_definitions = []
        if name in SPECIAL_METHOD_NAMES:
            param_definitions.append('const PandasDataFrame &df')
        param_definitions.extend([x.proto for x in definition.params])

        if not is_py_kwargs(method):
            param_definitions.append('shared_ptr<DuckDBPyConnection> conn = nullptr')
        param_definitions = ", ".join(param_definitions)

        param_names = [x.name for x in definition.params]
        param_names = ", ".join(param_names)

        function_name = definition.name
        if name in SPECIAL_METHOD_NAMES:
            function_name = 'FromDF(df)->' + function_name

        format_dict = {
            'param_definitions': param_definitions,
            'opt_retrieval': '',
            'opt_return': '' if definition.is_void else 'return ',
            'function_name': function_name,
            'parameter_names': param_names,
        }
        if is_py_kwargs(method):
            format_dict['opt_retrieval'] += RETRIEVE_CONN_FROM_DICT

        return LAMBDA_FORMAT.format_map(format_dict)

    def create_definition(name, method, lambda_def) -> str:
        definition = f"m.def(\"{name}\""
        definition += ", "
        definition += lambda_def
        definition += ", "
        definition += f"\"{method['docs']}\""
        if 'args' in method and not is_py_args(method):
            definition += ", "
            arguments = create_arguments(method['args'])
            definition += ', '.join(arguments)
        if 'kwargs' in method:
            definition += ", "
            if is_py_kwargs(method):
                definition += "py::kw_only()"
            else:
                definition += "py::kw_only(), "
                arguments = create_arguments(method['kwargs'])
                definition += ', '.join(arguments)
        definition += ");"
        return definition

    body = []
    all_names = []
    for method in connection_methods:
        if isinstance(method['name'], list):
            names = method['name']
        else:
            names = [method['name']]
        if 'kwargs' not in method:
            method['kwargs'] = []
        method['kwargs'].append({'name': 'connection', 'type': 'Optional[DuckDBPyConnection]', 'default': 'None'})
        for name in names:
            function_name = method['function']
            cpp_definition = cpp_connection_defs[function_name]
            lambda_def = get_lambda_definition(name, method, cpp_definition)
            body.append(create_definition(name, method, lambda_def))
            all_names.append(name)

    for method in wrapper_methods:
        if isinstance(method['name'], list):
            names = method['name']
        else:
            names = [method['name']]
        if 'kwargs' not in method:
            method['kwargs'] = []
        method['kwargs'].append({'name': 'connection', 'type': 'Optional[DuckDBPyConnection]', 'default': 'None'})
        for name in names:
            function_name = method['function']
            if name in SPECIAL_METHOD_NAMES:
                cpp_definition = cpp_relation_defs[function_name]
                if 'args' not in method:
                    method['args'] = []
                method['args'].insert(0, {'name': 'df', 'type': 'DataFrame'})
            else:
                cpp_definition = cpp_connection_defs[function_name]
            lambda_def = get_lambda_definition(name, method, cpp_definition)
            body.append(create_definition(name, method, lambda_def))
            all_names.append(name)

    # ---- End of generation code ----

    with_newlines = ['\t' + x + '\n' for x in body]
    # Recreate the file content by concatenating all the pieces together
    new_content = start_section + with_newlines + end_section
    # Write out the modified DUCKDB_PYTHON_SOURCE file
    with open(DUCKDB_PYTHON_SOURCE, 'w') as source_file:
        source_file.write("".join(new_content))

    item_list = '\n'.join([f'\t{name},' for name in all_names])
    str_item_list = '\n'.join([f"\t'{name}'," for name in all_names])
    imports = PY_INIT_FORMAT.format(item_list=item_list, str_item_list=str_item_list).split('\n')
    imports = [x + '\n' for x in imports]

    init_py_content = py_start + imports + py_end
    # Write out the modified DUCKDB_INIT_FILE file
    with open(DUCKDB_INIT_FILE, 'w') as source_file:
        source_file.write("".join(init_py_content))


if __name__ == '__main__':
    # raise ValueError("Please use 'generate_connection_code.py' instead of running the individual script(s)")
    generate()
