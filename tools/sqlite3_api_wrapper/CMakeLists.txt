include_directories(include)
add_subdirectory(sqlite3)

include_directories(sqlite3_udf_api/include)
add_subdirectory(sqlite3_udf_api)

add_extension_definitions()
add_definitions(-DSQLITE_SHELL_IS_UTF8)

include_directories(../../third_party/utf8proc/include)

set(SQLITE_API_WRAPPER_FILES
    sqlite3_api_wrapper.cpp sql_auto_complete_extension.cpp ${ALL_OBJECT_FILES})

add_library(sqlite3_api_wrapper_static STATIC ${SQLITE_API_WRAPPER_FILES})
target_link_libraries(sqlite3_api_wrapper_static duckdb_static duckdb_utf8proc)
link_threads(sqlite3_api_wrapper_static)

if(NOT WIN32)
  add_library(sqlite3_api_wrapper SHARED ${SQLITE_API_WRAPPER_FILES})
  target_link_libraries(sqlite3_api_wrapper duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(sqlite3_api_wrapper)

  include_directories(../../third_party/catch)

  include_directories(test/include)
  add_subdirectory(test)

  add_executable(test_sqlite3_api_wrapper ${SQLITE_TEST_FILES})
  target_link_libraries(test_sqlite3_api_wrapper sqlite3_api_wrapper)
endif()
