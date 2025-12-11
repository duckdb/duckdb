add_library_unity(duckdb_parquet_reader_variant OBJECT
                  variant_binary_decoder.cpp variant_shredded_conversion.cpp)

set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_reader_variant>
    PARENT_SCOPE)
