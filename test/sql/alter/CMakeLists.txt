add_library_unity(test_sql_alter
                  OBJECT
                  test_alter.cpp
                  test_rename_table.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_alter>
    PARENT_SCOPE)
