cmake_minimum_required(VERSION 2.8.7)

project(tpce CXX C)

include_directories(include)

set(CMAKE_BUILD_TYPE "Release")
add_subdirectory(main)
add_subdirectory(input)
add_subdirectory(utilities)

add_definitions(-DCOMPILE_FLAT_FILE_LOAD)

add_library(tpce STATIC
            ${TPCE_OBJECT_FILES}
            duckdb_interface.cpp
            tpce.cpp
            tpce_generated.cpp)

# disable warnings
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set_target_properties(
    tpce
    PROPERTIES
      COMPILE_FLAGS
      "-w"
    )
endif()