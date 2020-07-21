add_library_unity(test_sql_tpce OBJECT test_tpce.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_tpce>
    PARENT_SCOPE)
