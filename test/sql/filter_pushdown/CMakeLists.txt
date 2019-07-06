add_library_unity(test_filter_pushdown OBJECT test_filter_pushdown.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_filter_pushdown>
    PARENT_SCOPE)
