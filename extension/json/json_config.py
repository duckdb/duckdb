import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/json/include', 'extension/json/yyjson/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/json/buffered_json_reader.cpp', 'extension/json/json-extension.cpp', 'extension/json/json_common.cpp', 'extension/json/json_functions.cpp', 'extension/json/json_scan.cpp', 'extension/json/json_functions/json_array_length.cpp', 'extension/json/json_functions/json_contains.cpp', 'extension/json/json_functions/json_extract.cpp', 'extension/json/json_functions/json_merge_patch.cpp', 'extension/json/json_functions/json_structure.cpp', 'extension/json/json_functions/json_transform.cpp', 'extension/json/json_functions/json_create.cpp', 'extension/json/json_functions/json_type.cpp', 'extension/json/json_functions/json_valid.cpp', 'extension/json/json_functions/read_json_objects.cpp', 'extension/json/yyjson/yyjson.cpp']]
