add_library_unity(test_sql_schema OBJECT test_quoted_column_name.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_schema>
    PARENT_SCOPE)
