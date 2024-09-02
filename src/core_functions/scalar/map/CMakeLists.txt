add_library_unity(
  duckdb_func_map_nested
  OBJECT
  map.cpp
  map_extract.cpp
  map_from_entries.cpp
  map_entries.cpp
  map_concat.cpp
  map_contains.cpp
  map_keys_values.cpp
  cardinality.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_map_nested>
    PARENT_SCOPE)
