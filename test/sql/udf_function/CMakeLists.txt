add_library(test_sql_udf_function
                  OBJECT
                  test_templated_scalar_udf.cpp
                  test_argumented_scalar_udf.cpp
                  test_templated_vec_udf.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_udf_function>
    PARENT_SCOPE)
