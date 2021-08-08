add_library_unity(
  duckdb_main_capi
  OBJECT
  appender-c.cpp
  arrow-c.cpp
  config-c.cpp
  datetime-c.cpp
  duckdb-c.cpp
  helper-c.cpp
  hugeint-c.cpp
  prepared-c.cpp
  result-c.cpp
  value-c.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_capi>
    PARENT_SCOPE)
