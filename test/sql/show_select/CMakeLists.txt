add_library_unity(test_sql_show_select
                  OBJECT
                  test_show_select.cpp
                  )
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_show_select>
    PARENT_SCOPE)
