include_directories(../../third_party/dbgen/include)
include_directories(../../third_party/sqlite/include)
add_library(duckdb_benchmark_expression_reordering OBJECT
            benchmark_expressions.cpp
            adaptive_reordering.cpp)
set(BENCHMARK_OBJECT_FILES ${BENCHMARK_OBJECT_FILES}
                           $<TARGET_OBJECTS:duckdb_benchmark_expression_reordering> PARENT_SCOPE)
