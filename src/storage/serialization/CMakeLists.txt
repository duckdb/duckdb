add_library_unity(
  duckdb_storage_serialization
  OBJECT
  serialize_parsed_expression.cpp
  serialize_result_modifier.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_serialization>
    PARENT_SCOPE)
