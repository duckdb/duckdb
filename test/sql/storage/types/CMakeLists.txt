add_library_unity(test_sql_storage_type
  OBJECT
  test_blob.cpp
  test_timestamp.cpp)

set(ALL_OBJECT_FILES
${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_storage_type>
PARENT_SCOPE)
