add_library_unity(test_sql_explain OBJECT test_explain.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_explain>
    PARENT_SCOPE)
