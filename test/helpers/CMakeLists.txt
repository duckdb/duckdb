set(DUCKDB_TEST_HELPERS_UNITS test_helpers.cpp capi_tester.cpp pid.cpp
                              test_config.cpp)

add_library(test_helpers STATIC ${DUCKDB_TEST_HELPERS_UNITS})
