add_library_unity(
  duckdb_parquet_reader_variant OBJECT variant_binary_decoder.cpp
  parquet_variant_iterator.cpp parquet_variant_shredding.cpp)

set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_reader_variant>
    PARENT_SCOPE)
