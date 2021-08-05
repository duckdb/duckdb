include_directories(../../third_party/sqlite/include)
add_library(
  duckdb_benchmark_micro OBJECT append.cpp append_mix.cpp bulkupdate.cpp
                                cast.cpp in.cpp storage.cpp)
set(BENCHMARK_OBJECT_FILES
    ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_micro>
    PARENT_SCOPE)
