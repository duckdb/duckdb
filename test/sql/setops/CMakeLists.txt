add_library_unity(test_sql_setops
                  OBJECT
                  test_joins_under_setops.cpp
                  test_union.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_setops>
    PARENT_SCOPE)
