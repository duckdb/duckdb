add_library_unity(duckdb_core_functions_union OBJECT union_extract.cpp
                  union_value.cpp union_tag.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_union>
    PARENT_SCOPE)
