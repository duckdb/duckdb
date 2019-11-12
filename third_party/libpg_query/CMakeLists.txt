cmake_minimum_required(VERSION 2.8.7)

project(pg_query CXX C)
cmake_policy(SET CMP0063 NEW)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(pg_query STATIC
            postgres_parser.cpp
            pg_functions.cpp
            src_backend_parser_parser.c
            src_backend_parser_gram.c
            src_backend_nodes_list.c
            src_backend_parser_scansup.c
            src_backend_nodes_makefuncs.c
            src_backend_nodes_value.c
            src_backend_parser_scan.c
            src_common_keywords.c)

target_include_directories(pg_query PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(pg_query PROPERTIES
    EXPORT_NAME duckdb_pg_query)

install(
    TARGETS pg_query
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

if(NOT MSVC)
  set_target_properties(pg_query
                        PROPERTIES COMPILE_FLAGS
                                   "-Wno-unused-function -Wno-unused-variable")
endif()

set_target_properties(pg_query PROPERTIES LINKER_LANGUAGE C)
set_target_properties(pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)
