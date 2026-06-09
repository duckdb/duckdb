add_library_unity(duckdb_row_operations OBJECT row_aggregate.cpp
                  row_matcher.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_row_operations>
    PARENT_SCOPE)
