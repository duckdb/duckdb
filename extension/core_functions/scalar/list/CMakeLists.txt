add_library_unity(
  duckdb_func_list
  OBJECT
  flatten.cpp
  list_value.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_list>
    PARENT_SCOPE)
