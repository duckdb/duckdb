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
set(SQLITE_API_WRAPPER_FILES shell_extension.cpp sqlite3_api_wrapper.cpp
                             ${ALL_OBJECT_FILES})

add_library(sqlite3_api_wrapper_static STATIC ${SQLITE_API_WRAPPER_FILES})
target_link_libraries(sqlite3_api_wrapper_static duckdb_static)

include_directories(../../third_party/catch)

include_directories(test/include)
add_subdirectory(test)

add_executable(test_sqlite3_api_wrapper ${SQLITE_TEST_FILES})
target_link_libraries(test_sqlite3_api_wrapper sqlite3_api_wrapper_static)
link_extension_libraries(sqlite3_api_wrapper_static "")
