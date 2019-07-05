add_library_unity(duckdb_merge_join
                  OBJECT
                  merge_join.cpp
                  merge_join_inner.cpp
                  merge_join_mark.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_merge_join>
    PARENT_SCOPE)
