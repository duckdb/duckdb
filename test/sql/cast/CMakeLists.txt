add_library_unity(test_sql_cast OBJECT test_boolean_cast.cpp test_string_cast.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_cast>
    PARENT_SCOPE)
