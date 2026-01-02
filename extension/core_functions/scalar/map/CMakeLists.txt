add_library_unity(
  duckdb_core_functions_map
  OBJECT
  map_keys_values.cpp
  map_extract.cpp
  map_from_entries.cpp
  map_entries.cpp
  map.cpp
  map_concat.cpp
  switch.cpp
  cardinality.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_map>
    PARENT_SCOPE)
