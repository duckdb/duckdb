add_library_unity(test_persistence OBJECT test_file_matches_wal.cpp
                  test_locking.cpp test_persistence.cpp test_sequence_crash.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_persistence>
    PARENT_SCOPE)
