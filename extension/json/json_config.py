import os

# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/json/include']]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/json/buffered_json_reader.cpp',
        'extension/json/json_enums.cpp',
        'extension/json/json_extension.cpp',
        'extension/json/json_common.cpp',
        'extension/json/json_functions.cpp',
        'extension/json/json_scan.cpp',
        'extension/json/json_functions/copy_json.cpp',
        'extension/json/json_functions/json_array_length.cpp',
        'extension/json/json_functions/json_contains.cpp',
        'extension/json/json_functions/json_exists.cpp',
        'extension/json/json_functions/json_extract.cpp',
        'extension/json/json_functions/json_keys.cpp',
        'extension/json/json_functions/json_merge_patch.cpp',
        'extension/json/json_functions/json_pretty.cpp',
        'extension/json/json_functions/json_structure.cpp',
        'extension/json/json_functions/json_transform.cpp',
        'extension/json/json_functions/json_create.cpp',
        'extension/json/json_functions/json_type.cpp',
        'extension/json/json_functions/json_valid.cpp',
        'extension/json/json_functions/json_value.cpp',
        'extension/json/json_functions/read_json_objects.cpp',
        'extension/json/json_functions/read_json.cpp',
        'extension/json/json_functions/json_serialize_plan.cpp',
        'extension/json/json_functions/json_serialize_sql.cpp',
        'extension/json/json_serializer.cpp',
        'extension/json/json_deserializer.cpp',
        'extension/json/serialize_json.cpp',
    ]
]
