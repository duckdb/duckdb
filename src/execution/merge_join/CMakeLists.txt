add_library_unity(duckdb_merge_join
                  OBJECT
                  merge_join.cpp
        merge_join_complex.cpp
                  merge_join_simple.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_merge_join>
    PARENT_SCOPE)
