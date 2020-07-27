add_library_unity(test_sql_constraints
                  OBJECT
                  test_primarykey.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_constraints>
    PARENT_SCOPE)
