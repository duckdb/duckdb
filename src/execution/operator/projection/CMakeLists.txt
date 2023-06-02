add_library_unity(
  duckdb_operator_projection OBJECT physical_projection.cpp
  physical_tableinout_function.cpp physical_pivot.cpp physical_unnest.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_projection>
    PARENT_SCOPE)
