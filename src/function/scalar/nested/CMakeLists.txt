add_library_unity(duckdb_func_nested
                  OBJECT
                  list_value.cpp
                  struct_extract.cpp
                  struct_pack.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_nested>
    PARENT_SCOPE)
