add_subdirectory(cast)

add_library_unity(
  duckdb_main_capi
  OBJECT
  appender-c.cpp
  arrow-c.cpp
  config-c.cpp
  data_chunk-c.cpp
  datetime-c.cpp
  duckdb-c.cpp
  duckdb_value-c.cpp
  helper-c.cpp
  hugeint-c.cpp
  logical_types-c.cpp
  pending-c.cpp
  prepared-c.cpp
  replacement_scan-c.cpp
  result-c.cpp
  stream-c.cpp
  table_function-c.cpp
  threading-c.cpp
  value-c.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_capi>
    PARENT_SCOPE)
