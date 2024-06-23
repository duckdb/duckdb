import os
import json
import re
import glob

DEPRECATED_FUNCTIONS = ['duckdb_row_count', 'duckdb_column_data', 'duckdb_nullmask_data', 'duckdb_result_get_chunk', 'duckdb_result_is_streaming', 'duckdb_result_chunk_count', 'duckdb_execute_prepared_streaming', 'duckdb_pending_prepared_streaming']
DEPRECATED_GROUPS = ['safe_fetch_functions', 'arrow_interface']

### Read duckdb.h
capi_path = 'src/include/duckdb.h'
with open(capi_path, 'r') as f:
    duckdb_capi = f.read()

### strip everything except the functions
functions_mark = '''//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//'''
duckdb_capi = duckdb_capi[duckdb_capi.find(functions_mark)+len(functions_mark):]

end_of_functions_mark='''#endif
//===--------------------------------------------------------------------===//
// End Of Functions
//===--------------------------------------------------------------------===//
'''
duckdb_capi = duckdb_capi[:duckdb_capi.find(end_of_functions_mark)]

def parse_function(function_string):
    function_string_trimmed = function_string[function_string.find("DUCKDB_API ")+len("DUCKDB_API "):]
    start_of_function_pos_in_capi = duckdb_capi.find(function_string)
    start_of_comment_in_capi = duckdb_capi.rfind("/*!", 0, start_of_function_pos_in_capi)
    comment = duckdb_capi[start_of_comment_in_capi:start_of_function_pos_in_capi].strip()

    if (comment[0:3] != '/*!' or comment[-2:] != '*/'):
        print(comment)
        print(f"Failed to parse comment for function: {function_string_trimmed}")
        exit(1)

    idx = 2 # skip duckdb_
    while not function_string_trimmed[idx:].startswith('duckdb_'):
        idx+=1

    type_end = idx
    type = function_string_trimmed[:type_end]

    while not function_string_trimmed[idx:].startswith('('):
        idx+=1

    name_end = idx
    function_name = function_string_trimmed[type_end:name_end]

    while not function_string_trimmed[idx:].startswith(')'):
        idx+=1

    param_list_end = idx

    param_list = function_string_trimmed[name_end+1:param_list_end].split(',')
    parsed_list = []
    for param in param_list:
        parsed_list.append(re.split(r"(?<=[\*\s])(?=[A-z\_]*$)", param))

    result = {}
    result['name'] = function_name.strip()
    result['return_type'] = type.strip()
    result['params'] = []

    if result['name'] in DEPRECATED_FUNCTIONS:
        result['deprecated'] = True

    for param in parsed_list:
        if param == ['']:
            continue
        result['params'].append({
            "type": param[0].strip(),
            "name": param[1].strip(),
        })

    # Now we parse the comment part
    description = re.findall(r"(?<=\/\*\!\s)[\s\S]+?(?=\*\s|\*\/)", comment)
    comment_param_list = re.findall(r"(?<=\s\*\s)([^:]*:[^*]*)", comment)
    comment_param_list = [x.split(':') for x in comment_param_list]

    if len(description) > 0:
        result['comment'] = {
            'description' : description[0]
        }

    for param in comment_param_list:
        if param[0] == 'returns':
            result['comment']['return_value'] = param[1].strip()
            continue

        # check this param even exists
        key_exists = next((x for x in result['params'] if x['name'] == param[0]), None)

        check_exceptions = [
            # 'duckdb_set_config', # Name mismatches
            # 'duckdb_value_varchar', # Decprecation notice parsed as param
            # 'duckdb_value_varchar_internal', # Decprecation notice parsed as param
            # 'duckdb_value_string_internal', # Decprecation notice parsed as param
            # 'duckdb_from_time_tz', # Comments don't match actual params
            # 'duckdb_extract_statements_error', # Name mismatches
            # 'duckdb_pending_error', # Name mismatches
            # 'duckdb_create_varchar', # Name mismatches
            # 'duckdb_create_varchar_length', # Name mismatches
            # 'duckdb_create_int64', # Name mismatches
            # 'duckdb_create_map_type', # Name mismatches: param missing comment
            # 'duckdb_create_union_type', # Name mismatches: param missing comment
            # 'duckdb_create_enum_type', # Non-existing param
            # 'duckdb_list_vector_reserve', # Return instead of returns
            # 'duckdb_destroy_scalar_function', # Name mismatch
            # 'duckdb_scalar_function_set_name', # Name mismatch
            # 'duckdb_scalar_function_set_function', # Name mismatch
            # 'duckdb_register_scalar_function', # Name mismatch
            # 'duckdb_bind_set_bind_data', # Name mismatches: param missing comment
            # 'duckdb_init_set_init_data', # Name mismatch
            # 'duckdb_appender_column_count', # missing :
            # 'duckdb_appender_column_type', # missing :
            # 'duckdb_prepared_arrow_schema', # name mismatch
            # 'duckdb_destroy_arrow_stream', # name mismatch
        ]
        if not key_exists and function_name not in check_exceptions:
            print(f'failed to parse comment for function {function_name}. The param: {param[0]} was not found in param list {result['params']}')
            exit(1)

        if not 'param_comments' in result['comment']:
            result['comment']['param_comments'] = {}

        result['comment']['param_comments'][param[0]] = param[1].strip()


    # print(f'successfully parsed comment:\n{comment}\ninto:\n{result['comment']}\n')

    return result

def get_functions_from_cpp_snippet(snippet, group_name, output_file):
    funs = re.findall("(?:DUCKDB_API [\s\S]+?(?=;);)", snippet)

    function_list = []
    for function in funs:
        # print(f"parsing function {function}")
        function_list.append(parse_function(function))

    return {
        "group" : group_name,
        "deprecated": group_name in DEPRECATED_GROUPS,
        "entries" : function_list
    }

groups = duckdb_capi.split("//===--------------------------------------------------------------------===//\n// ")

all_functions = []

# Create the groups
for group in groups:
    group_name = group[:group.find("\n")]
    group_name = group_name.replace('/', ' ')
    group_name = group_name.replace(' ', '_')
    group_name = group_name.lower()

    if group_name == "":
        continue
    print(group_name)

    group_file = f'src/include/duckdb/main/capi/header_generation/functions/{group_name}.json'
    function_group = get_functions_from_cpp_snippet(group, group_name, group_file)

    for function in function_group['entries']:
        all_functions.append(function)

    json_string = json.dumps(function_group, indent=4)
    with open(group_file, 'w+') as f:
        f.write(json_string)

# create first API version based on all functions
def create_api_v0():
    api_v0_obj = {
        'version_entries': [
            {
                'version': 'v0.0.1',
                'entries': [{'name': function['name']} for function in all_functions]
            }
        ]
    }
    api_v0_file = 'src/include/duckdb/main/capi/header_generation/apis/extension_api_v0.json'
    with open(api_v0_file, 'w+') as f:
        f.write( json.dumps(api_v0_obj, indent=4))

create_api_v0()