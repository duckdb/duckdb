cmake_minimum_required(VERSION 2.8.12)

project(TPCHExtension)

include_directories(dbgen/include)
include_directories(include)
add_subdirectory(dbgen)

add_library(tpch_extension STATIC tpch-extension.cpp ${DBGEN_OBJECT_FILES})
# target_link_libraries(tpch_extension dbgen)

# TODO make all this a support function
# skip building extensions on mingw
if(WIN32 AND NOT MSVC)
  return()
endif()

add_library(tpch_extension_shared SHARED tpch-extension.cpp
                                         ${DBGEN_OBJECT_FILES})
set_target_properties(tpch_extension_shared PROPERTIES OUTPUT_NAME "tpch")
set_target_properties(tpch_extension_shared PROPERTIES PREFIX "")
target_link_libraries(tpch_extension_shared duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
set_target_properties(tpch_extension_shared PROPERTIES SUFFIX
                                                       ".duckdb_extension")

if(MSVC)
  set_target_properties(
    tpch_extension_shared PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                                     ${CMAKE_BINARY_DIR}/extension/tpch)
  set_target_properties(
    tpch_extension_shared PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                                     ${CMAKE_BINARY_DIR}/extension/tpch)
endif()

if(WIN32)
  set_target_properties(tpch_extension_shared
                        PROPERTIES LINK_FLAGS_RELEASE "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
endif()
