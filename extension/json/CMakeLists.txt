cmake_minimum_required(VERSION 2.8.12)

project(JSONExtension)

include_directories(yyjson/include)
include_directories(include)
add_subdirectory(yyjson)
add_subdirectory(json_functions)

set(JSON_EXTENSION_FILES
    json-extension.cpp
    json_common.cpp
    json_functions/json_array_length.cpp
    json_functions/json_extract.cpp
    json_functions/json_merge_patch.cpp
    json_functions/json_structure.cpp
    json_functions/json_transform.cpp
    json_functions/json_create.cpp
    json_functions/json_type.cpp
    json_functions/json_valid.cpp
    ${YYJSON_OBJECT_FILES})

add_library(json_extension STATIC ${JSON_EXTENSION_FILES})
set(INGORE_WARNINGS OFF)
build_loadable_extension(json ${JSON_EXTENSION_FILES} ${IGNORE_WARNINGS})

install(
  TARGETS json_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
