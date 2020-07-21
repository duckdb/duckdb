add_library_unity(test_sql_append OBJECT test_big_append.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_append>
    PARENT_SCOPE)
