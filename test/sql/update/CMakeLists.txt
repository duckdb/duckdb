add_library_unity(test_sql_update
                  OBJECT
                  test_big_update.cpp
                  test_null_update.cpp
                  test_string_update.cpp
                  test_update.cpp
                  test_update_mix.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_update>
    PARENT_SCOPE)
