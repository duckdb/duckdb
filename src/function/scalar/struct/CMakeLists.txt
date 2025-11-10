add_library_unity(
  duckdb_func_struct_main
  OBJECT
  remap_struct.cpp
  struct_extract.cpp
  struct_pack.cpp
  struct_concat.cpp
  struct_contains.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_struct_main>
    PARENT_SCOPE)
