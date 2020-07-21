add_library_unity(test_sql_catalog
                  OBJECT
                  test_catalog_errors.cpp
                  test_create_from_select.cpp
                  test_schema.cpp
                  test_table.cpp
                  test_temporary.cpp
                  test_unicode_schema.cpp
                  test_dependencies.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_catalog>
    PARENT_SCOPE)
