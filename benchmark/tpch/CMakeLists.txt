include_directories(../../third_party/sqlite/include)
include_directories(../../extension/parquet/include)

set(BENCHMARK_SOURCE_FILES
    startup.cpp)

add_library(duckdb_benchmark_tpch OBJECT ${BENCHMARK_SOURCE_FILES})

set(BENCHMARK_OBJECT_FILES ${BENCHMARK_OBJECT_FILES}
                           $<TARGET_OBJECTS:duckdb_benchmark_tpch> PARENT_SCOPE)
