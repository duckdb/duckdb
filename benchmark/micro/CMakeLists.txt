
add_library(duckdb_benchmark_micro OBJECT groupby.cpp multiplications.cpp orderby.cpp rangejoin.cpp)
set(BENCHMARK_OBJECT_FILES ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_micro> PARENT_SCOPE)
