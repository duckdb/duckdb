add_library_unity(test_encryption OBJECT test_encryption_key_wiping.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_encryption>
    PARENT_SCOPE)
