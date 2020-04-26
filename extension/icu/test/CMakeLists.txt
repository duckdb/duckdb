
include_directories(../../../third_party/catch)
include_directories(../../../test/include)

add_executable(icu_test icu_test.cpp)
target_link_libraries(icu_test icu_extension duckdb test_helpers)
