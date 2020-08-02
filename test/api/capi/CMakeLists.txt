add_library_unity(test_sql_capi
                  OBJECT
                  test_capi.cpp
                  test_starting_database.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi>
    PARENT_SCOPE)
