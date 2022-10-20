add_library_unity(test_filter OBJECT filter_cache.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_filter>
    PARENT_SCOPE)
