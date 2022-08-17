set(DUCKDB_TEST_HELPERS_UNITS test_helpers.cpp test_helper_extension.cpp
                              capi_tester.cpp pid.cpp)

add_library(test_helpers STATIC ${DUCKDB_TEST_HELPERS_UNITS})

if(NOT WIN32)
  if (STATIC_LINK_UNITTEST)
    target_link_libraries(test_helpers duckdb_static)
  elseif()
    target_link_libraries(test_helpers duckdb)
  endif()
endif()
