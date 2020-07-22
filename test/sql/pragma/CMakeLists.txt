add_library_unity(test_sql_pragma
                  OBJECT
                  test_pragma.cpp
                  test_show_tables.cpp
                  test_table_info.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_pragma>
    PARENT_SCOPE)
