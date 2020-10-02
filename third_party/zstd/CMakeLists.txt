cmake_minimum_required(VERSION 2.8.7)

project(zstd CXX C)
cmake_policy(SET CMP0063 NEW)

include_directories(.)

set(CMAKE_BUILD_TYPE "Release")
add_subdirectory(common)
add_subdirectory(decompress)

add_definitions(-DZDICT_STATIC_LINKING_ONLY)

add_library(zstd STATIC ${ZSTD_OBJECT_FILES})

disable_target_warnings(zstd)
