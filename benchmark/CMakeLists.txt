include_directories(../third_party/catch)
include_directories(../third_party/tpce-tool/include)
include_directories(../third_party/sqlite/include)
include_directories(../test/include)
include_directories(include)

add_subdirectory(micro)
add_subdirectory(tpch)
add_subdirectory(tpcds)
add_subdirectory(imdb)
add_subdirectory(expression_reordering)

add_executable(benchmark_runner benchmark_runner.cpp interpreted_benchmark.cpp ${BENCHMARK_OBJECT_FILES})

target_link_libraries(
  benchmark_runner
  duckdb
  dbgen
  dsdgen
  imdb
  test_helpers)

if (${BUILD_TPCE})
  target_link_libraries(benchmark_runner tpce)
endif()

if(${BUILD_PARQUET_EXTENSION})
  target_link_libraries(benchmark_runner parquet_extension)
endif()
