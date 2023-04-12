cmake_minimum_required(VERSION 2.8.12)

project(AutoCompleteExtension)

include_directories(include)

set(AUTOCOMPLETE_EXTENSION_FILES sql_auto_complete-extension.cpp)

add_library(autocomplete_extension STATIC ${AUTOCOMPLETE_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(autocomplete ${PARAMETERS}
                         ${AUTOCOMPLETE_EXTENSION_FILES})

install(
  TARGETS autocomplete_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
