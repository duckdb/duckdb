add_library_unity(test_wal_reader OBJECT wal_reader_test.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_wal_reader>
    PARENT_SCOPE)
