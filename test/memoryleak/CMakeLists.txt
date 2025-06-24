add_library_unity(test_memory_leak OBJECT test_appender.cpp
                  test_temporary_tables.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_memory_leak>
    PARENT_SCOPE)
