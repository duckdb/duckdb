add_library_unity(duckdb_parquet_writer_variant OBJECT convert_variant.cpp)

set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_writer_variant>
    PARENT_SCOPE)
