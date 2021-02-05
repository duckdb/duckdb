include_directories(include)
add_subdirectory(sqlite3)

add_extension_definitions()
add_definitions(-DSQLITE_SHELL_IS_UTF8)

add_library(sqlite3_api_wrapper_static STATIC sqlite3_api_wrapper.cpp
                                              ${ALL_OBJECT_FILES})
target_link_libraries(sqlite3_api_wrapper_static duckdb_static)
link_threads(sqlite3_api_wrapper_static)

link_extension_libraries(sqlite3_api_wrapper_static)

if(NOT WIN32)
  add_library(sqlite3_api_wrapper SHARED sqlite3_api_wrapper.cpp
                                         ${ALL_OBJECT_FILES})
  target_link_libraries(sqlite3_api_wrapper duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(sqlite3_api_wrapper)

  link_extension_libraries(sqlite3_api_wrapper)

  include_directories(../../third_party/catch)

  add_executable(test_sqlite3_api_wrapper test_sqlite3_api_wrapper.cpp)
  target_link_libraries(test_sqlite3_api_wrapper sqlite3_api_wrapper)
endif()
