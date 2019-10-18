add_library_unity(test_filter_predicate OBJECT debug_predicate_query.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_filter_predicate>
    PARENT_SCOPE)
