add_subdirectory(aggregate)
add_subdirectory(cast)
add_subdirectory(pragma)
add_subdirectory(scalar)
add_subdirectory(table)

add_library_unity(
  duckdb_function
  OBJECT
  built_in_functions.cpp
  cast_rules.cpp
  compression_config.cpp
  function.cpp
  function_binder.cpp
  function_set.cpp
  pragma_function.cpp
  macro_function.cpp
  scalar_macro_function.cpp
  table_macro_function.cpp
  scalar_function.cpp
  table_function.cpp
  udf_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_function>
    PARENT_SCOPE)
