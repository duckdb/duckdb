add_library_unity(test_appender
                  OBJECT
                  test_appender_abort.cpp
                  test_appender.cpp
                  test_concurrent_append.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_appender>
    PARENT_SCOPE)
