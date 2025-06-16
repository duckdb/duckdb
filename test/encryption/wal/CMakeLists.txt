add_library_unity(test_wal_encryption
        OBJECT
        test_basic_wal_encryption.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_wal_encryption>
        PARENT_SCOPE)