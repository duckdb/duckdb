add_library_unity(test_sql_update OBJECT test_update.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_update>
    PARENT_SCOPE)
