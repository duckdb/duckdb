cmake_minimum_required(VERSION 2.8.7)

project(TPCHExtension)

include_directories(dbgen/include)
include_directories(include)
add_subdirectory(dbgen)

add_library(tpch_extension STATIC tpch-extension.cpp)
target_link_libraries(tpch_extension dbgen)
