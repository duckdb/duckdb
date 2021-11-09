add_library_unity(
  duckdb_storage_segment
  OBJECT
  numeric_constant.cpp
  fixed_size_uncompressed.cpp
  rle.cpp
  string_uncompressed.cpp
  uncompressed.cpp
  validity_uncompressed.cpp
        bitpacking.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_segment>
    PARENT_SCOPE)
