include_directories(../../../third_party/catch)
include_directories(../../../test/include)

add_executable(icu_test icu_test.cpp)

if(NOT WIN32 AND NOT SUN)
  target_link_libraries(icu_test
                        icu_extension
                        duckdb
                        test_helpers)
else()
target_link_libraries(icu_test
                      icu_extension
                      duckdb_static
                      test_helpers)
endif()
