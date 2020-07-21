add_library_unity(test_sql_views OBJECT test_views.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_views>
    PARENT_SCOPE)
