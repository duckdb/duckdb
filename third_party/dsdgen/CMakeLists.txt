cmake_minimum_required(VERSION 2.8.12)

project(dsdgen CXX)
cmake_policy(SET CMP0063 NEW)
include_directories(include)

add_subdirectory(dsdgen-c)

set(CMAKE_BUILD_TYPE "Release")
add_library(dsdgen STATIC
            dsdgen.cpp
            append_info-c.cpp
            dsdgen_helpers.cpp
            ${ALL_OBJECT_FILES})

disable_target_warnings(dsdgen)
