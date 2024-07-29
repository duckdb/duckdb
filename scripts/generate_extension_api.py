import os
import json
import re
import glob
import copy
from packaging.version import Version

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

# Whether the script allows functions with parameters without a comment explaining them
ALLOW_UNCOMMENTED_PARAMS = True

DUCKDB_EXT_API_PTR_NAME = 'duckdb_ext_api'
DUCKDB_EXT_API_STRUCT_NAME = 'duckdb_ext_api_v0'
EXT_API_DEFINITION_FILE = 'src/include/duckdb/main/capi/header_generation/apis/extension_api_v0.json'

# The main C api header of DuckDB
DUCKDB_HEADER_OUT_FILE = 'src/include/duckdb.h'
# The header to be included by DuckDB C extensions
DUCKDB_HEADER_EXT_OUT_FILE = 'src/include/duckdb_extension.h'
# The internal header for DuckDB to work with the CAPI
DUCKDB_HEADER_EXT_INTERNAL_OUT_FILE = 'src/include/duckdb/main/capi/extension_api.hpp'

# The JSON files that define all available CAPI functions
CAPI_FUNCTION_DEFINITION_FILES = 'src/include/duckdb/main/capi/header_generation/functions/**/*.json'

# The original order of the function groups in the duckdb.h files. We maintain this for easier PR reviews.
ORIGINAL_FUNCTION_GROUP_ORDER = [
    'open_connect',
    'configuration',
    'query_execution',
    'result_functions',
    'safe_fetch_functions',
    'helpers',
    'date_time_timestamp_helpers',
    'hugeint_helpers',
    'unsigned_hugeint_helpers',
    'decimal_helpers',
    'prepared_statements',
    'bind_values_to_prepared_statements',
    'execute_prepared_statements',
    'extract_statements',
    'pending_result_interface',
    'value_interface',
    'logical_type_interface',
    'data_chunk_interface',
    'vector_interface',
    'validity_mask_functions',
    'scalar_functions',
    'table_functions',
    'table_function_bind',
    'table_function_init',
    'table_function',
    'replacement_scans',
    'appender',
    'arrow_interface',
    'threading_information',
    'streaming_result_interface',
]

# The file that forms the base for the header generation
BASE_HEADER_TEMPLATE = 'src/include/duckdb/main/capi/header_generation/header_base.hpp'
# The comment marking where this script will inject its contents
BASE_HEADER_CONTENT_MARK = '// DUCKDB_FUNCTIONS_ARE_GENERATED_HERE\n'


# Loads the template for the header files to be generated
def fetch_header_template_main():
    # Read the base header
    with open(BASE_HEADER_TEMPLATE, 'r') as f:
        result = f.read()

    # Trim the base header
    header_mark = '// DUCKDB_START_OF_HEADER\n'
    if header_mark not in result:
        print(f"Could not find the header start mark: {header_mark}")
        exit(1)

    return result[result.find(header_mark) + len(header_mark) :]

def fetch_header_template_ext():
        return """#pragma once
        
#include "duckdb.h"

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//

// DUCKDB_FUNCTIONS_ARE_GENERATED_HERE
"""


# Parse the CAPI_FUNCTION_DEFINITION_FILES to get the full list of functions
def parse_capi_function_definitions():
    # Collect all functions
    function_files = glob.glob(CAPI_FUNCTION_DEFINITION_FILES, recursive=True)

    function_groups = []
    function_map = {}

    # Read functions
    for file in function_files:
        with open(file, 'r') as f:
            try:
                json_data = json.loads(f.read())
            except json.decoder.JSONDecodeError as err:
                print(f"Invalid JSON found in {file}: {err}")
                exit(1)

            function_groups.append(json_data)
            for function in json_data['entries']:
                if function['name'] in function_map:
                    print(f"Duplicate symbol found when parsing C API file {file}: {function['name']}")
                    exit(1)

                function['group'] = json_data['group']
                if 'deprecated' in json_data:
                    function['group_deprecated'] = json_data['deprecated']
                if 'excluded_from_struct' in json_data:
                    function['group_excluded_from_struct'] = json_data['excluded_from_struct']

                function_map[function['name']] = function

    return (function_groups, function_map)


# Read extension API
def parse_ext_api_definition():
    with open(EXT_API_DEFINITION_FILE, 'r') as f:
        try:
            return json.loads(f.read())
        except json.decoder.JSONDecodeError as err:
            print(f"Invalid JSON found in {extension_api_v0_file}: {err}")
            exit(1)


# Creates the comment that accompanies describing a C api function
def create_function_comment(function_obj):
    result = ''

    function_name = function_obj['name']
    # Construct comment
    if 'comment' in function_obj:
        comment = function_obj['comment']
        result += '/*!\n'
        result += comment['description']
        # result += '\n\n'
        if 'params' in function_obj:
            for param in function_obj['params']:
                param_name = param['name']
                if not 'param_comments' in comment:
                    if not ALLOW_UNCOMMENTED_PARAMS:
                        print(comment)
                        print(f'Missing param comments for function {function_name}')
                        exit(1)
                    continue
                if param['name'] in comment['param_comments']:
                    param_comment = comment['param_comments'][param['name']]
                    result += f'* {param_name}: {param_comment}\n'
                elif not ALLOW_UNCOMMENTED_PARAMS:
                    print(f'Uncommented parameter found: {param_name} of function {function_name}')
                    exit(1)
        if 'return_value' in comment:
            comment_return_value = comment['return_value']
            result += f'* returns: {comment_return_value}\n'
        result += '*/\n'
    return result


# Creates the function declaration for the regular C header file
def create_function_declaration(function_obj):
    result = ''
    function_name = function_obj['name']
    function_return_type = function_obj['return_type']

    # Construct function declaration
    result += f'DUCKDB_API {function_return_type}'
    if result[-1] != '*':
        result += ' '
    result += f'{function_name}('

    if 'params' in function_obj:
        if len(function_obj['params']) > 0:
            for param in function_obj['params']:
                param_type = param['type']
                param_name = param['name']
                result += f'{param_type}'
                if result[-1] != '*':
                    result += ' '
                result += f'{param_name}, '
            result = result[:-2]  # Trailing comma
    result += ');\n'

    return result


# Creates the function declaration for extension api struct
def create_struct_member(function_obj):
    result = ''

    function_name = function_obj['name']
    function_return_type = function_obj['return_type']
    result += f'    {function_return_type} (*{function_name})('
    if 'params' in function_obj:
        if len(function_obj['params']) > 0:
            for param in function_obj['params']:
                param_type = param['type']
                param_name = param['name']
                result += f'{param_type} {param_name},'
            result = result[:-1]  # Trailing comma
    result += ');'

    return result


# Creates the function declaration for extension api struct
def create_function_typedef(function_obj):
    function_name = function_obj['name']
    return f'#define {function_name} {DUCKDB_EXT_API_PTR_NAME}->{function_name}\n'


def to_camel_case(snake_str):
    return " ".join(x.capitalize() for x in snake_str.lower().split("_"))


def parse_semver(version):
    if version[0] != 'v':
        print(f"Version string {version} does not start with a v")
        exit(1)

    versions = version[1:].split(".")

    if len(versions) != 3:
        print(f"Version string {version} is invalid, only vx.y.z is supported")
        exit(1)

    return int(versions[0]), int(versions[1]), int(versions[2])


def create_version_defines(version):
    major, minor, patch = parse_semver(EXT_API_VERSION)
    version_string = f'v{major}.{minor}.{patch}'

    result = f"#define DUCKDB_EXTENSION_API_VERSION \"{version_string}\"\n"
    result += f"#define DUCKDB_EXTENSION_API_VERSION_MAJOR {major}\n"
    result += f"#define DUCKDB_EXTENSION_API_VERSION_MINOR {minor}\n"
    result += f"#define DUCKDB_EXTENSION_API_VERSION_PATCH {patch}\n"

    return result

# Create duckdb.h
def create_duckdb_h(ext_api_version):
    function_declarations_finished = ''

    function_groups_copy = copy.deepcopy(FUNCTION_GROUPS)

    if len(function_groups_copy) != len(ORIGINAL_FUNCTION_GROUP_ORDER):
        print("original order list is wrong")

    for order_group in ORIGINAL_FUNCTION_GROUP_ORDER:
        # Lookup the group in the original order: purely intended to keep the PR review sane
        curr_group = next(group for group in function_groups_copy if group['group'] == order_group)
        function_groups_copy.remove(curr_group)

        function_declarations_finished += f'''//===--------------------------------------------------------------------===//
// {to_camel_case(curr_group['group'])}
//===--------------------------------------------------------------------===//\n\n'''

        if 'description' in curr_group:
            function_declarations_finished += curr_group['description'] + '\n'

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

    header_template = fetch_header_template_main()
    duckdb_h = DUCKDB_H_HEADER + header_template.replace(BASE_HEADER_CONTENT_MARK, function_declarations_finished)
    with open(DUCKDB_HEADER_OUT_FILE, 'w+') as f:
        f.write(duckdb_h)


def create_extension_api_struct(ext_api_version, with_create_method=False):
    # Generate the struct
    extension_struct_finished = 'typedef struct {\n'
    for api_version_entry in EXT_API_DEFINITION['version_entries']:
        version = api_version_entry['version']
        extension_struct_finished += f'    // Version {version}\n'
        for function in api_version_entry['entries']:
            function_lookup = FUNCTION_MAP[function['name']]
            # Special case for C API functions that were already deprecated when the extension API started
            if 'deprecated' in function_lookup and function_lookup['deprecated'] and 'excluded_from_struct' in function_lookup and function_lookup['excluded_from_struct']:
                continue
            if 'group_deprecated' in function_lookup and function_lookup['group_deprecated'] and 'group_excluded_from_struct' in function_lookup and function_lookup['group_excluded_from_struct']:
                continue

            # TODO: comments inside the struct or no?
            # extension_struct_finished += create_function_comment(function_lookup)
            extension_struct_finished += create_struct_member(function_lookup)
            extension_struct_finished += '\n'
    extension_struct_finished += '} ' + f'{DUCKDB_EXT_API_STRUCT_NAME};\n\n'

    if with_create_method:
        extension_struct_finished += "inline duckdb_ext_api_v0 CreateApi() {\n"
        extension_struct_finished += "    return {\n"
        for api_version_entry in EXT_API_DEFINITION['version_entries']:
            for function in api_version_entry['entries']:
                function_lookup = FUNCTION_MAP[function['name']]
                # Special case for C API functions that were already deprecated when the extension API started
                if 'deprecated' in function_lookup and function_lookup['deprecated'] and 'excluded_from_struct' in function_lookup and function_lookup['excluded_from_struct']:
                    continue
                if 'group_deprecated' in function_lookup and function_lookup['group_deprecated'] and 'group_excluded_from_struct' in function_lookup and function_lookup['group_excluded_from_struct']:
                    continue
                function_lookup_name = function_lookup['name']
                extension_struct_finished += f'        {function_lookup_name}, \n'
        extension_struct_finished = extension_struct_finished[:-1]
        extension_struct_finished += "    };\n"
        extension_struct_finished += "}\n\n"

    extension_struct_finished += create_version_defines(ext_api_version)
    extension_struct_finished += "\n"

    return extension_struct_finished


# Create duckdb_extension_api.h
def create_duckdb_ext_h(ext_api_version):

    # Generate the typedefs
    typedefs = ""
    for group in FUNCTION_GROUPS:
        # Special case for C API functions that were already deprecated when the extension API started
        if 'deprecated' in group and group['deprecated'] and 'excluded_from_struct' in group and group['excluded_from_struct']:
            continue

        group_name = group['group']
        typedefs += f'//! {group_name}\n'
        if 'deprecated' in group and group['deprecated']:
            typedefs += f'#ifndef DUCKDB_API_NO_DEPRECATED\n'

        for function in group['entries']:
            if 'deprecated' in function and function['deprecated']:
                # Special case for C API functions that were already deprecated when the extension API started
                if 'excluded_from_struct' in function and function['excluded_from_struct']:
                    continue
                typedefs += '#ifndef DUCKDB_API_NO_DEPRECATED\n'

            typedefs += create_function_typedef(function)

            if 'deprecated' in function and function['deprecated']:
                typedefs += '#endif\n'

        if 'deprecated' in group and group['deprecated']:
            typedefs += '#endif\n'
        typedefs += '\n'

    extension_header_body = create_extension_api_struct(ext_api_version) + '\n\n' + typedefs

    extension_header_body += f'// Place in global scope of C/C++ file that contains the DUCKDB_EXTENSION_REGISTER_ENTRYPOINT call\n'
    extension_header_body += (
        f'# define DUCKDB_EXTENSION_GLOBAL const {DUCKDB_EXT_API_STRUCT_NAME} *{DUCKDB_EXT_API_PTR_NAME}=0;\n'
    )
    extension_header_body += f'// Place in global scope of any C/C++ file that needs to access the extension API\n'
    extension_header_body += (
        f'# define DUCKDB_EXTENSION_EXTERN extern const {DUCKDB_EXT_API_STRUCT_NAME} *{DUCKDB_EXT_API_PTR_NAME};\n'
    )
    extension_header_body += f'// Initializes the C Extension API: First thing to call in the extension entrypoint\n'
    extension_header_body += f' #define DUCKDB_EXTENSION_API_INIT(info, access) {DUCKDB_EXT_API_PTR_NAME} = ({DUCKDB_EXT_API_STRUCT_NAME} *) access->get_api(info, DUCKDB_EXTENSION_API_VERSION); if (!{DUCKDB_EXT_API_PTR_NAME}) {{ return; }};\n'
    extension_header_body += f'// Register the extension entrypoint\n'
    extension_header_body += ' #define DUCKDB_EXTENSION_REGISTER_ENTRYPOINT(extension_name, entrypoint) DUCKDB_EXTENSION_API void extension_name##_init_c_api(duckdb_extension_info info, duckdb_extension_access *access) { DUCKDB_EXTENSION_API_INIT(info, access); duckdb_database *db = access->get_database(info); duckdb_connection conn; if (duckdb_connect(*db, &conn) == DuckDBError) { access->set_error(info, "Failed to open connection to database"); return; } entrypoint(conn, info, access); duckdb_disconnect(&conn);}\n'


    header_template = fetch_header_template_ext()
    duckdb_ext_h = DUCKDB_EXT_H_HEADER + header_template.replace(BASE_HEADER_CONTENT_MARK, extension_header_body)
    with open(DUCKDB_HEADER_EXT_OUT_FILE, 'w+') as f:
        f.write(duckdb_ext_h)

# Create duckdb_extension_internal.hpp
def create_duckdb_ext_internal_h(ext_api_version):
    extension_header_body = create_extension_api_struct(ext_api_version, with_create_method=True)
    header_template = fetch_header_template_ext()
    duckdb_ext_h = DUCKDB_EXT_H_HEADER + header_template.replace(BASE_HEADER_CONTENT_MARK, extension_header_body)
    with open(DUCKDB_HEADER_EXT_INTERNAL_OUT_FILE, 'w+') as f:
        f.write(duckdb_ext_h)

def get_extension_api_version():
    versions = []

    for version_entry in EXT_API_DEFINITION['version_entries']:
        versions.append(version_entry['version'])

    versions_copy = copy.deepcopy(versions)

    versions.sort(key=Version)

    if versions != versions_copy:
        print("Failed to parse extension api: versions are not ordered correctly")
        exit(1)

    return versions[-1]


# TODO make this code less spaghetti
if __name__ == "__main__":
    EXT_API_DEFINITION = parse_ext_api_definition()
    EXT_API_VERSION = get_extension_api_version()
    FUNCTION_GROUPS, FUNCTION_MAP = parse_capi_function_definitions()

    print("Generating C APIs")
    print(f" * Extension API Version: {EXT_API_VERSION}")

    print("Generating header: " + DUCKDB_HEADER_OUT_FILE)
    create_duckdb_h(EXT_API_VERSION)
    print("Generating header: " + DUCKDB_HEADER_EXT_OUT_FILE)
    create_duckdb_ext_h(EXT_API_VERSION)
    print("Generating header: " + DUCKDB_HEADER_EXT_INTERNAL_OUT_FILE)
    create_duckdb_ext_internal_h(EXT_API_VERSION)

    os.system(f"python3 scripts/format.py {DUCKDB_HEADER_OUT_FILE} --fix --noconfirm")
    os.system(f"python3 scripts/format.py {DUCKDB_HEADER_EXT_OUT_FILE} --fix --noconfirm")
    os.system(f"python3 scripts/format.py {DUCKDB_HEADER_EXT_INTERNAL_OUT_FILE} --fix --noconfirm")
