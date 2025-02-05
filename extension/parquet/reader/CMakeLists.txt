add_library_unity(duckdb_parquet_readers OBJECT string_column_reader.cpp struct_column_reader.cpp)
set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_readers>
    PARENT_SCOPE)
