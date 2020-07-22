add_library_unity(test_sql_constraints
                  OBJECT
                  test_check.cpp
                  test_notnull.cpp
                  test_primarykey.cpp
                  test_unique.cpp
                  test_update.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_constraints>
    PARENT_SCOPE)
