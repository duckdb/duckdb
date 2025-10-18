add_library_unity(
  test_persistence
  OBJECT
  test_file_matches_wal.cpp
  test_locking.cpp
  test_persistence.cpp
  test_sequence_crash.cpp
  test_add_column_with_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_persistence>
    PARENT_SCOPE)
