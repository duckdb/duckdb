add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}"
                -DDUCKDB_BUILD_DIRECTORY="${PROJECT_BINARY_DIR}")

set(DUCKDB_TEST_HELPERS_UNITS test_helpers.cpp capi_tester.cpp pid.cpp
                              test_config.cpp)

add_library(test_helpers STATIC ${DUCKDB_TEST_HELPERS_UNITS})

if(NOT WIN32)
  target_link_libraries(test_helpers duckdb)
endif()
