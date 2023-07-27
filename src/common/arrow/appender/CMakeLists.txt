add_library_unity(
  duckdb_common_arrow_appender
  OBJECT
  bool_data.cpp
  list_data.cpp
  map_data.cpp
  struct_data.cpp
  union_data.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_arrow_appender>
    PARENT_SCOPE)
