add_library_unity(test_sql_prepared OBJECT test_prepared.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_prepared>
    PARENT_SCOPE)
