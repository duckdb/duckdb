# skip building extensions on mingw
if(WIN32 AND NOT MSVC)
  return()
endif()

add_library(loadable_extension_demo SHARED loadable_extension_demo.cpp)
set_target_properties(loadable_extension_demo PROPERTIES PREFIX "")
target_link_libraries(loadable_extension_demo duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
set_target_properties(loadable_extension_demo PROPERTIES SUFFIX
                                                         ".duckdb_extension")

if(MSVC)
  set_target_properties(
    loadable_extension_demo PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                                       ${CMAKE_BINARY_DIR}/test/extension)
  set_target_properties(
    loadable_extension_demo PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                                       ${CMAKE_BINARY_DIR}/test/extension)
endif()

if(WIN32)
  set_target_properties(loadable_extension_demo
                        PROPERTIES LINK_FLAGS_RELEASE "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
endif()
