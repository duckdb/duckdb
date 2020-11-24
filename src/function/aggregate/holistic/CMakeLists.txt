add_library_unity(duckdb_aggr_holistic
                  OBJECT
                 median.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_holistic>
    PARENT_SCOPE)
