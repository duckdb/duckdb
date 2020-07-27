add_library_unity(test_sql_catalog
                  OBJECT
                  test_schema.cpp
                  test_temporary.cpp
                  test_dependencies.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_catalog>
    PARENT_SCOPE)
