include_directories(../../third_party/dbgen/include)
include_directories(../../third_party/sqlite/include)
add_library(duckdb_benchmark_optimizer OBJECT
            bench_like_optimizer.cpp)
set(BENCHMARK_OBJECT_FILES ${BENCHMARK_OBJECT_FILES}
                           $<TARGET_OBJECTS:duckdb_benchmark_optimizer> PARENT_SCOPE)
