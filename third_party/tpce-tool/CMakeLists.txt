cmake_minimum_required(VERSION 2.8.7)

include_directories(include)

project(tpce CXX C)

include_directories(include)

add_subdirectory(main)
add_subdirectory(input)
add_subdirectory(utilities)

add_definitions(-DCOMPILE_FLAT_FILE_LOAD)

set(CMAKE_BUILD_TYPE "Release")
add_library(tpce STATIC ${TPCE_OBJECT_FILES})
target_link_libraries(tpce duckdb)


