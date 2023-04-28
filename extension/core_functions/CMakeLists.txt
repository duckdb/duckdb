cmake_minimum_required(VERSION 2.8.12)

project(CoreFunctionsExtension)

include_directories(include)
add_subdirectory(scalar)

set(CORE_FUNCTIONS_SOURCES
    core_functions_extension.cpp function_list.cpp ${ALL_OBJECT_FILES})

add_library(core_functions_extension STATIC ${CORE_FUNCTIONS_SOURCES})
set(PARAMETERS "-warnings")
build_loadable_extension(core_functions ${PARAMETERS} ${CORE_FUNCTIONS_SOURCES})

install(
  TARGETS core_functions_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
