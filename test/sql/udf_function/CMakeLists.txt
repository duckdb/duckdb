add_library_unity(test_sql_UDF_function
                  OBJECT
                  test_templated_UDF_function.cpp
                  test_argumented_UDF_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_UDF_function>
    PARENT_SCOPE)
