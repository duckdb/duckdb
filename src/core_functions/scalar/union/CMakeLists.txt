add_library_unity(duckdb_func_union OBJECT union_extract.cpp union_tag.cpp
                  union_value.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_union>
    PARENT_SCOPE)
