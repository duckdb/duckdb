
add_library(duckdb_benchmark_micro OBJECT groupby.cpp multiplications.cpp orderby.cpp pointquery.cpp indexcreation.cpp rangejoin.cpp rangequery.cpp)
set(BENCHMARK_OBJECT_FILES ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_micro> PARENT_SCOPE)
