add_subdirectory(catalog)
add_library_unity(
  test_sql_storage
  OBJECT
  test_buffer_manager.cpp
  test_checksum.cpp
  test_big_storage.cpp
  test_repeated_checkpoint.cpp
  test_storage.cpp
  test_database_size.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_storage>
    PARENT_SCOPE)
