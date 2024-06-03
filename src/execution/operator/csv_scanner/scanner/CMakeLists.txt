add_library_unity(
  duckdb_csv_scanner
  OBJECT
  base_scanner.cpp
  column_count_scanner.cpp
  csv_schema.cpp
  scanner_boundary.cpp
  skip_scanner.cpp
  string_value_scanner.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_csv_scanner>
    PARENT_SCOPE)
