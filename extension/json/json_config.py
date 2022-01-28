import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/json/include', 'extension/json/yyjson/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/json/json-extension.cpp', 'extension/json/json_common.cpp', 'extension/json/json_extract.cpp', 'extension/json/json_type.cpp', 'extension/json/json_valid.cpp', 'extension/json/yyjson/yyjson.cpp']]
