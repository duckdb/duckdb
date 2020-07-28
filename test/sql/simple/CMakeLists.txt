add_library_unity(test_sql_simple
                  OBJECT
                  test_join.cpp
                  test_window.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_simple>
    PARENT_SCOPE)
