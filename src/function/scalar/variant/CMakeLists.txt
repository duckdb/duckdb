add_library_unity(
  duckdb_func_variant_main
  OBJECT
  variant_utils.cpp
  variant_array_length.cpp
  variant_bind_utils.cpp
  variant_exists.cpp
  variant_extract.cpp
  variant_keys.cpp
  variant_typeof.cpp
  variant_normalize.cpp
  variant_path_function.cpp
  variant_type.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_variant_main>
    PARENT_SCOPE)
