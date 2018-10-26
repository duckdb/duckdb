
include_directories(include)

add_subdirectory(micro)

add_executable(benchmark_runner benchmark_runner.cpp ${BENCHMARK_OBJECT_FILES})
target_link_libraries(benchmark_runner duckdb dbgen tpce)
