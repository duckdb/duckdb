SET(DUCKDB_TEST_HELPERS_UNITS test_helpers.cpp) # expression_helper.cpp 

add_library(test_helpers STATIC ${DUCKDB_TEST_HELPERS_UNITS})

IF (NOT WIN32)
  target_link_libraries(test_helpers duckdb)
ENDIF()