cmake_minimum_required(VERSION 2.8.7)

project(dsdgen CXX)
CMAKE_POLICY(SET CMP0063 NEW)
include_directories(include)

add_subdirectory(dsdgen-c)

set(CMAKE_BUILD_TYPE "Release")
add_library(dsdgen STATIC dsdgen.cpp append_info-c.cpp dsdgen_helpers.cpp ${ALL_OBJECT_FILES})
