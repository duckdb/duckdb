add_library_unity(
  duckdb_parquet_readers
  OBJECT
  cast_column_reader.cpp
  decimal_column_reader.cpp
  expression_column_reader.cpp
  list_column_reader.cpp
  row_number_column_reader.cpp
  string_column_reader.cpp
  struct_column_reader.cpp)
set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_readers>
    PARENT_SCOPE)
