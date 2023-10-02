include_directories(include)
add_subdirectory(sqlite3)

include_directories(sqlite3_udf_api/include)
add_subdirectory(sqlite3_udf_api)

add_extension_definitions()
add_definitions(-DSQLITE_SHELL_IS_UTF8)
add_definitions(-DUSE_DUCKDB_SHELL_WRAPPER)

include_directories(../../third_party/utf8proc/include)

if(DUCKDB_EXTENSION_AUTOCOMPLETE_SHOULD_LINK)
  include_directories(../../extension/autocomplete/include)
  set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                       ../../extension/autocomplete/autocomplete_extension.cpp)
  add_definitions(-DSHELL_INLINE_AUTOCOMPLETE)
endif()
set(SQLITE_API_WRAPPER_FILES sqlite3_api_wrapper.cpp ${ALL_OBJECT_FILES})

add_library(sqlite3_api_wrapper_static STATIC ${SQLITE_API_WRAPPER_FILES})
target_link_libraries(sqlite3_api_wrapper_static duckdb_static)
if(NOT AMALGAMATION_BUILD)
  target_link_libraries(sqlite3_api_wrapper_static duckdb_utf8proc)
endif()
link_threads(sqlite3_api_wrapper_static)

if(NOT WIN32 AND NOT ZOS)
  add_library(sqlite3_api_wrapper SHARED ${SQLITE_API_WRAPPER_FILES})
  target_link_libraries(sqlite3_api_wrapper duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(sqlite3_api_wrapper)
endif()

include_directories(../../third_party/catch)

include_directories(test/include)
add_subdirectory(test)

add_executable(test_sqlite3_api_wrapper ${SQLITE_TEST_FILES})
if(WIN32 OR ZOS)
  target_link_libraries(test_sqlite3_api_wrapper sqlite3_api_wrapper_static)
else()
  target_link_libraries(test_sqlite3_api_wrapper sqlite3_api_wrapper)
endif()
