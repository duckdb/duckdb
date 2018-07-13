cmake_minimum_required(VERSION 2.8.7)

# ---[ Peloton project
project(pg_query CXX C)

# this code imitates the Makefile in /third_party/libpg_query/
file(GLOB_RECURSE pg_query_srcs ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
list(REMOVE_ITEM pg_query_srcs
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_fingerprint_defs.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_fingerprint_conds.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_json_defs.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_json_conds.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/postgres/guc-file.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/postgres/scan.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_json_helper.c")

include_directories(.)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/postgres/include)

add_library(pg_query STATIC ${pg_query_srcs})

set_target_properties(pg_query PROPERTIES LINKER_LANGUAGE C)
set_target_properties(pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)