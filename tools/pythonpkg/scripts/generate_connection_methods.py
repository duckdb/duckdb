import os
import json

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
PYCONNECTION_SOURCE = os.path.join("..", "src", "pyconnection.cpp")

INITIALIZE_METHOD = (
    "static void InitializeConnectionMethods(py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>> &m) {"
)
END_MARKER = "} // END_OF_CONNECTION_METHODS"


def is_py_kwargs(method):
    return 'kwargs_as_dict' in method and method['kwargs_as_dict'] == True


def is_py_args(method):
    if 'args' not in method:
        return False
    args = method['args']
    if len(args) == 0:
        return False
    if args[0]['name'] != '*args':
        return False
    return True


def generate():
    # Read the PYCONNECTION_SOURCE file
    with open(PYCONNECTION_SOURCE, 'r') as source_file:
        source_code = source_file.readlines()

    start_index = -1
    end_index = -1
    for i, line in enumerate(source_code):
        if line.startswith(INITIALIZE_METHOD):
            if start_index != -1:
                raise ValueError("Encountered the INITIALIZE_METHOD a second time, quitting!")
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
                break
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

    def create_definition(name, method) -> str:
        definition = f"m.def(\"{name}\""
        definition += ", "
        definition += f"""&DuckDBPyConnection::{method['function']}"""
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

    # Write out the modified PYCONNECTION_SOURCE file
    with open(PYCONNECTION_SOURCE, 'w') as source_file:
        source_file.write("".join(new_content))


if __name__ == '__main__':
    raise ValueError("Please use 'generate_connection_code.py' instead of running the individual script(s)")
    # generate()
