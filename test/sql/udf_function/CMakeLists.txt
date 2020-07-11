add_library_unity(test_sql_udf_function
                  OBJECT
                  test_templated_udf_function.cpp
                  test_argumented_udf_function.cpp
                  test_vectorized_udf_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_udf_function>
    PARENT_SCOPE)
