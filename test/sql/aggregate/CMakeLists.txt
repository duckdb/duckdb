add_library_unity(test_sql_aggregate
                  OBJECT
                  test_aggregate.cpp
                  test_aggregate_types.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_aggregate>
    PARENT_SCOPE)
