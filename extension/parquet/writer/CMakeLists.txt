add_library_unity(
  duckdb_parquet_writers
  OBJECT
  struct_column_writer.cpp)
set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_writers>
    PARENT_SCOPE)
