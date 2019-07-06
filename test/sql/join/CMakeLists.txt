add_library_unity(test_sql_join
                  OBJECT
                  test_join_on_aggregates.cpp
                  test_left_outer_join.cpp
                  test_unequal_join.cpp
                  test_varchar_join.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_join>
    PARENT_SCOPE)
