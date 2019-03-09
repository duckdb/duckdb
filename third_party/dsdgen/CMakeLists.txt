cmake_minimum_required(VERSION 2.8.7)

add_subdirectory(dsdgen-c)

project(dsdgen CXX)
include_directories(include)

add_library(dsdgen STATIC dsdgen.cpp append_info-c.cpp dsdgen_helpers.cpp)
target_link_libraries(dsdgen dsdgen_c)


