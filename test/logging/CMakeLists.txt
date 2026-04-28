add_library_unity(test_logging OBJECT test_logging.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_logging>
    PARENT_SCOPE)
