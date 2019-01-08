SET(DUCKDB_TEST_HELPERS_UNITS expression_helper.cpp capi_helpers.cpp test_helpers.cpp)

IF (NOT WIN32)
  SET(DUCKDB_TEST_HELPERS_UNITS${DUCKDB_TEST_HELPERS_UNITS} sqlite_helpers.cpp)
ENDIF()

add_library(test_helpers STATIC ${DUCKDB_TEST_HELPERS_UNITS})

IF (NOT WIN32)
  target_link_libraries(test_helpers duckdb sqlite3)
ENDIF()