include_directories(../../../third_party/catch)
include_directories(../../../test/include)

add_executable(parquet_test parquet_test.cpp)

if(NOT WIN32 AND NOT SUN)
  target_link_libraries(parquet_test parquet_extension duckdb test_helpers)
else()
  target_link_libraries(parquet_test parquet_extension duckdb_static
                        test_helpers)
endif()
