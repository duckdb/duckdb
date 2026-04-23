add_library_unity(test_filter OBJECT filter_cache.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_filter>
    PARENT_SCOPE)
