add_library_unity(test_persistence OBJECT test_persistence.cpp test_locking.cpp
                  test_sequence_crash.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_persistence>
    PARENT_SCOPE)
