set(SSB_BENCHMARK_SOURCE_FILES ssb_benchmark.cpp)

add_library(duckdb_benchmark_ssb OBJECT ${SSB_BENCHMARK_SOURCE_FILES})

set(BENCHMARK_OBJECT_FILES
    ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_ssb>
    PARENT_SCOPE)