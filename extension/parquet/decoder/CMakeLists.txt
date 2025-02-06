add_library_unity(
  duckdb_parquet_decoders
  OBJECT
  delta_binary_packed_decoder.cpp
  dictionary_decoder.cpp
  rle_decoder.cpp)
set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_decoders>
    PARENT_SCOPE)
