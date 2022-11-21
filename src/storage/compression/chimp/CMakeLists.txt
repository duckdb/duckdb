add_library_unity(
  duckdb_storage_compression_chimp
  OBJECT
  bit_reader.cpp
  chimp_constants.cpp
  flag_buffer.cpp
  leading_zero_buffer.cpp
  chimp.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression_chimp>
    PARENT_SCOPE)
