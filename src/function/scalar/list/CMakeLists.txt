add_library_unity(duckdb_func_list_nested OBJECT list_concat.cpp
                  contains_or_position.cpp list_extract.cpp list_resize.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_list_nested>
    PARENT_SCOPE)
