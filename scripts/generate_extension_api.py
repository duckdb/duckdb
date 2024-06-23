import os
import json
import re
import glob

DUCKDB_H_HEADER = '''//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
//
//===----------------------------------------------------------------------===//
'''

DUCKDB_EXT_H_HEADER = '''//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb_extension.h
//
//
//===----------------------------------------------------------------------===//
'''

ALLOW_UNCOMMENTED_PARAMS = True

# Read the base header
with open("src/include/duckdb/main/capi/header_generation/header_base.hpp", 'r') as f:
    HEADER_TEMPLATE = f.read()

# Trim the base header
header_mark = '// DUCKDB_START_OF_HEADER\n'
if header_mark not in HEADER_TEMPLATE:
    print(f"Could not find the header start mark: {header_mark}")
    exit(1)

HEADER_TEMPLATE = HEADER_TEMPLATE[HEADER_TEMPLATE.find(header_mark)+len(header_mark):]

functions_mark = '// DUCKDB_FUNCTIONS_ARE_GENERATED_HERE\n'


# Collect all functions
function_files = glob.glob('src/include/duckdb/main/capi/header_generation/functions/**/*.json', recursive=True)

function_groups = []
function_map = {}

original_order = [ 'open_connect', 'configuration', 'query_execution', 'result_functions', 'safe_fetch_functions', 'helpers', 'date_time_timestamp_helpers', 'hugeint_helpers', 'unsigned_hugeint_helpers', 'decimal_helpers', 'prepared_statements', 'bind_values_to_prepared_statements', 'execute_prepared_statements', 'extract_statements', 'pending_result_interface', 'value_interface', 'logical_type_interface', 'data_chunk_interface', 'vector_interface', 'validity_mask_functions', 'scalar_functions', 'table_functions', 'table_function_bind', 'table_function_init', 'table_function', 'replacement_scans', 'appender', 'arrow_interface', 'threading_information', 'streaming_result_interface' ]

# Read functions
for file in function_files:
    with open(file, 'r') as f:
        try:
            json_data = json.loads(f.read())
        except json.decoder.JSONDecodeError as err:
            print(f"Invalid JSON found in {file}: {err}")
            exit(1)

        # TODO verify
        function_groups.append(json_data)
        for function in json_data['entries']:
            if function['name'] in function_map:
                print(f"Duplicate symbol found when parsing C API file {file}: {function['name']}")
                exit(1)
            function_map[function['name']] = function

# Read extension API
extension_api_v0_file = 'src/include/duckdb/main/capi/header_generation/apis/extension_api_v0.json'
with open(extension_api_v0_file, 'r') as f:
    try:
        extension_api_v0 = json.loads(f.read())
    except json.decoder.JSONDecodeError as err:
        print(f"Invalid JSON found in {extension_api_v0_file}: {err}")
        exit(1)

# Creates the comment that accompanies describing a C api function
def create_function_comment(function_obj):
    result = ''
    # Construct comment
    if 'comment' in function_obj:
        comment = function_obj['comment']
        result += '/*!\n'
        result += comment['description']
        # result += '\n\n'
        if 'params' in function_obj:
            for param in function_obj['params']:
                if not 'param_comments' in comment:
                    if not ALLOW_UNCOMMENTED_PARAMS:
                        print(comment)
                        print(f'Missing param comments for function {function_obj['name']}')
                        exit(1)
                    continue
                if param['name'] in comment['param_comments']:
                    result += f'* {param['name']}: {comment['param_comments'][param['name']]}\n'
                elif not ALLOW_UNCOMMENTED_PARAMS:
                    print(f'Uncommented parameter found: {param['name']} of function {function['name']}')
                    exit(1)
        if 'return_value' in comment:
            result += f'* returns: {comment['return_value']}\n'
        result += '*/\n'
    return result

# Creates the function declaration for the regular C header file
def create_function_declaration(function_obj):
    result = ''
    # Construct function declaration
    result += f'DUCKDB_API {function_obj['return_type']}'
    if result[-1] != '*':
        result += ' '
    result += f'{function_obj['name']}('

    if 'params' in function_obj:
        if len(function_obj['params']) > 0:
            for param in function_obj['params']:
                result += f'{param['type']}'
                if result[-1] != '*':
                    result += ' '
                result += f'{param['name']}, '
            result = result[:-2] # Trailing comma
    result += ');\n'

    return result

# Creates the function declaration for extension api struct
def create_struct_member(function_obj):
    result = ''
    result += f'    {function_obj['return_type']} (*{function_obj['name']})('
    if 'params' in function_obj:
        if len(function_obj['params']) > 0:
            for param in function_obj['params']:
                result += f'{param['type']} {param['name']},'
            result = result[:-1] # Trailing comma
    result += ');'

    return result

def to_camel_case(snake_str):
    return " ".join(x.capitalize() for x in snake_str.lower().split("_"))

# Create duckdb.h
def create_duckdb_h():
    function_declarations_finished = ''

    function_groups_copy = function_groups

    if len(function_groups_copy) != len(original_order):
        print("original order list is wrong")

    for order_group in original_order:
        # Lookup the
        curr_group = next(group for group in function_groups_copy if group['group'] == order_group)
        function_groups_copy.remove(curr_group)

        function_declarations_finished += f'''//===--------------------------------------------------------------------===//
// {to_camel_case(curr_group['group'])}
//===--------------------------------------------------------------------===//\n\n'''

        if 'deprecated' in curr_group and curr_group['deprecated']:
            function_declarations_finished += f'#ifndef DUCKDB_API_NO_DEPRECATED\n'

        for function in curr_group['entries']:
            if 'deprecated' in function and function['deprecated']:
                function_declarations_finished += '#ifndef DUCKDB_API_NO_DEPRECATED\n'

            function_declarations_finished += create_function_comment(function)
            function_declarations_finished += create_function_declaration(function)

            if 'deprecated' in function and function['deprecated']:
                function_declarations_finished += '#endif\n'

            function_declarations_finished += '\n'


        if 'deprecated' in curr_group and curr_group['deprecated']:
            function_declarations_finished += '#endif\n'

    duckdb_h = DUCKDB_H_HEADER + HEADER_TEMPLATE.replace(functions_mark, function_declarations_finished)
    with open('src/include/duckdb.h', 'w+') as f:
        f.write(duckdb_h)

# Create duckdb_extension_api.h
def create_duckdb_ext_h():
    # Generate the struct
    extension_struct_finished = 'typedef struct {\n'
    for api_version_entry in extension_api_v0['version_entries']:
        extension_struct_finished += f'    // Version {api_version_entry['version']}\n'

        for function in  api_version_entry['entries']:
            function_lookup = function_map[function['name']]
            # TODO: comments inside the struct or no?
            # extension_struct_finished += create_function_comment(function_lookup)
            extension_struct_finished += create_struct_member(function_lookup)
            extension_struct_finished += '\n'
    extension_struct_finished += '} duckdb_ext_api_v0;\n\n'

    # Generate the typedefs


    duckdb_ext_h = DUCKDB_EXT_H_HEADER + HEADER_TEMPLATE.replace(functions_mark, extension_struct_finished)
    with open('src/include/duckdb_extension.h', 'w+') as f:
        f.write(duckdb_ext_h)

create_duckdb_h()
create_duckdb_ext_h()