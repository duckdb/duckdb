add_subdirectory(chimp)
add_subdirectory(alp)
add_subdirectory(roaring)
add_subdirectory(dictionary)
add_subdirectory(dict_fsst)

add_library_unity(
  duckdb_storage_compression
  OBJECT
  numeric_constant.cpp
  fixed_size_uncompressed.cpp
  rle.cpp
  dictionary_compression.cpp
  string_uncompressed.cpp
  uncompressed.cpp
  validity_uncompressed.cpp
  bitpacking.cpp
  bitpacking_hugeint.cpp
  patas.cpp
  zstd.cpp
  alprd.cpp
  fsst.cpp
  dict_fsst.cpp
  empty_validity.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression>
    PARENT_SCOPE)
