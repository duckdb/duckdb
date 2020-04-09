include_directories(../../third_party/dbgen/include)
include_directories(../../third_party/sqlite/include)
add_library(
  duckdb_benchmark_micro OBJECT
  append.cpp
  bulkupdate.cpp
  cast.cpp
        data_skipping.cpp
  groupby.cpp
  in.cpp
  multiplications.cpp
  orderby.cpp
  pointquery.cpp
  indexcreation.cpp
  rangejoin.cpp
  rangequery.cpp
  string.cpp
  window.cpp
  storage.cpp
  prefix_tpch.cpp
  suffix_tpch.cpp
  contains_tpch.cpp)
set(BENCHMARK_OBJECT_FILES
    ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_micro>
    PARENT_SCOPE)
