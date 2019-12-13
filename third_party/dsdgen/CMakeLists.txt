cmake_minimum_required(VERSION 2.8.7)

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

# disable warnings
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set_target_properties(
    dsdgen
    PROPERTIES
      COMPILE_FLAGS
      "-w"
    )
endif()