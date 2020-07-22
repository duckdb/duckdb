add_library_unity(test_nested OBJECT test_struct.cpp test_list.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_nested>
    PARENT_SCOPE)
