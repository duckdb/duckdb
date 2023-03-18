set(DUCKDB_TEST_HELPERS_UNITS test_helpers.cpp capi_tester.cpp pid.cpp)

add_library(test_helpers STATIC ${DUCKDB_TEST_HELPERS_UNITS})

if(NOT WIN32)
  target_link_libraries(test_helpers duckdb)
endif()
