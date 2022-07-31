import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/json/include', 'extension/json/yyjson/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/json/json-extension.cpp', 'extension/json/json_common.cpp', 'extension/json/json_functions/json_array_length.cpp', 'extension/json/json_functions/json_extract.cpp', 'extension/json/json_functions/json_merge_patch.cpp', 'extension/json/json_functions/json_structure.cpp', 'extension/json/json_functions/json_transform.cpp', 'extension/json/json_functions/json_create.cpp', 'extension/json/json_functions/json_type.cpp', 'extension/json/json_functions/json_valid.cpp', 'extension/json/yyjson/yyjson.cpp']]
