add_library_unity(test_optimizer OBJECT union_alls.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer>
    PARENT_SCOPE)
