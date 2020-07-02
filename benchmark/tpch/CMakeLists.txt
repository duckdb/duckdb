include_directories(../../third_party/dbgen/include)
include_directories(../../third_party/sqlite/include)
include_directories(../../extension/parquet/include)

set(BENCHMARK_SOURCE_FILES lineitem_aggregate.cpp sf1.cpp sf1-parallel.cpp read_lineitem.cpp
                           startup.cpp)

if(${BUILD_PARQUET_EXTENSION})
  set(BENCHMARK_SOURCE_FILES ${BENCHMARK_SOURCE_FILES} lineitem_parquet.cpp)
endif()

add_library(duckdb_benchmark_tpch OBJECT ${BENCHMARK_SOURCE_FILES})

set(BENCHMARK_OBJECT_FILES
    ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_tpch>
    PARENT_SCOPE)
