cmake_minimum_required(VERSION 2.8.12...3.29)

project(AutoCompleteExtension)

include_directories(include)

set(AUTOCOMPLETE_EXTENSION_FILES
    autocomplete_extension.cpp matcher.cpp tokenizer.cpp keyword_helper.cpp
    keyword_map.cpp)

add_subdirectory(transformer)
add_subdirectory(parser)

build_static_extension(autocomplete ${AUTOCOMPLETE_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(autocomplete ${PARAMETERS}
                         ${AUTOCOMPLETE_EXTENSION_FILES})

install(
  TARGETS autocomplete_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
