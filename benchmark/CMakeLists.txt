
include_directories(../third_party/dbgen/include)
include_directories(../third_party/tpce-tool/include)
include_directories(../test/include)
include_directories(include)

add_subdirectory(micro)
add_subdirectory(tpch)

add_executable(benchmark_runner benchmark_runner.cpp ${BENCHMARK_OBJECT_FILES})
target_link_libraries(benchmark_runner duckdb dbgen tpce test_helpers)
