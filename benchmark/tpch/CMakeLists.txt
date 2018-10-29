
add_library(duckdb_benchmark_tpch OBJECT sf1.cpp)
set(BENCHMARK_OBJECT_FILES ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_tpch> PARENT_SCOPE)
