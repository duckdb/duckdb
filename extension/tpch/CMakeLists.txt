cmake_minimum_required(VERSION 2.8.12)

project(TPCHExtension)

include_directories(dbgen/include)
include_directories(include)
add_subdirectory(dbgen)

add_library(tpch_extension STATIC tpch-extension.cpp ${DBGEN_OBJECT_FILES})
# target_link_libraries(tpch_extension dbgen)

build_loadable_extension(tpch tpch-extension.cpp ${DBGEN_OBJECT_FILES})
