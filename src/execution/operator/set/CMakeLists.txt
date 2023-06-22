add_library_unity(duckdb_operator_set OBJECT physical_union.cpp
                  physical_recursive_cte.cpp physical_cte.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_set>
    PARENT_SCOPE)
