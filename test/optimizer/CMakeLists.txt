add_library_unity(test_optimizer OBJECT union_alls.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer>
    PARENT_SCOPE)
