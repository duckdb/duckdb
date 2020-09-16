add_subdirectory(information_schema)
add_subdirectory(sqlite)
add_subdirectory(version)
add_library_unity(
  duckdb_func_table
  OBJECT
  arrow.cpp
  range.cpp
  repeat.cpp
  copy_csv.cpp
  read_csv.cpp
  sqlite_functions.cpp
  information_schema_functions.cpp
  table_scan.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_table>
    PARENT_SCOPE)
