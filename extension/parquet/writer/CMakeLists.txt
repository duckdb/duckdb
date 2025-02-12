add_library_unity(
  duckdb_parquet_writers
  OBJECT
  array_column_writer.cpp
  boolean_column_writer.cpp
  decimal_column_writer.cpp
  enum_column_writer.cpp
  list_column_writer.cpp
  primitive_column_writer.cpp
  struct_column_writer.cpp)
set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_writers>
    PARENT_SCOPE)
