cmake_minimum_required(VERSION 2.8.7)

project(pg_query CXX C)
CMAKE_POLICY(SET CMP0063 NEW)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

# this code imitates the Makefile in /third_party/libpg_query/
file(GLOB_RECURSE pg_query_srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.c)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(pg_query STATIC postgres_parser.cpp pg_functions.cpp src_backend_parser_parser.c src_backend_parser_gram.c src_backend_nodes_list.c src_backend_parser_scansup.c src_backend_nodes_makefuncs.c  src_backend_nodes_value.c src_backend_parser_scan.c  src_common_keywords.c)

IF (NOT MSVC)
  set_target_properties(pg_query PROPERTIES COMPILE_FLAGS "-Wno-unused-function -Wno-unused-variable")
ENDIF()

set_target_properties(pg_query PROPERTIES LINKER_LANGUAGE C)
set_target_properties(pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)