add_library_unity(test_sql_delete OBJECT test_delete.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_delete>
    PARENT_SCOPE)
