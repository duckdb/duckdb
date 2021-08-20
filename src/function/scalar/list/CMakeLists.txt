add_library_unity(duckdb_func_list OBJECT array_slice.cpp list_extract.cpp
                  list_value.cpp range.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_list>
    PARENT_SCOPE)
