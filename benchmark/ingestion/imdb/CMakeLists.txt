include_directories(../../third_party/imdb/include)

add_library(duckdb_benchmark_imdb OBJECT imdb.cpp)
set(BENCHMARK_OBJECT_FILES
    ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_imdb>
    PARENT_SCOPE)
