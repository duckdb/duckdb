add_library_unity(test_encryption OBJECT test_encryption_key_wiping.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_encryption>
    PARENT_SCOPE)
