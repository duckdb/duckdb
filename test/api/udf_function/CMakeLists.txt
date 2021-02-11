add_library_unity(
  test_api_udf_function
  OBJECT
  test_templated_scalar_udf.cpp
  test_argumented_scalar_udf.cpp
  test_templated_vec_udf.cpp
  test_argumented_vec_udf.cpp
  test_aggregate_udf.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_api_udf_function>
    PARENT_SCOPE)
