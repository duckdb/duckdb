add_library_unity(
  duckdb_json_functions
  OBJECT
  json_array_length.cpp
  json_extract.cpp
  json_merge_patch.cpp
  json_structure.cpp
  json_transform.cpp
  json_create.cpp
  json_type.cpp
  json_valid.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_json_functions>
    PARENT_SCOPE)
