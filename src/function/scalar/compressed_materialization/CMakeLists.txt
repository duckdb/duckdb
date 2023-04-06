add_library_unity(
  duckdb_func_compressed_materialization
  OBJECT
  compressed_materialization_integral.cpp
  compressed_materialization_string.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_compressed_materialization>
    PARENT_SCOPE)
