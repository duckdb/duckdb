add_library_unity(
  duckdb_parquet_decoders
  OBJECT
  byte_stream_split_decoder.cpp
  delta_binary_packed_decoder.cpp
  delta_byte_array_decoder.cpp
  delta_length_byte_array_decoder.cpp
  dictionary_decoder.cpp
  rle_decoder.cpp)
set(PARQUET_EXTENSION_FILES
    ${PARQUET_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_parquet_decoders>
    PARENT_SCOPE)
