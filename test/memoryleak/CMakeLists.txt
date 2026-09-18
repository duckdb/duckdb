add_library_unity(test_memory_leak OBJECT test_appender.cpp
                  test_temporary_tables.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_memory_leak>
    PARENT_SCOPE)
