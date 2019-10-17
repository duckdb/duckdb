include_directories(../../third_party/dbgen/include)
include_directories(../../third_party/sqlite/include)
add_library(duckdb_benchmark_predicate_optimization OBJECT
            predicate_optimization.cpp)
set(BENCHMARK_OBJECT_FILES ${BENCHMARK_OBJECT_FILES}
                           $<TARGET_OBJECTS:duckdb_benchmark_predicate_optimization> PARENT_SCOPE)
