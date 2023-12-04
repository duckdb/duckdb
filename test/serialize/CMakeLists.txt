add_library_unity(test_serialization OBJECT serialization_test.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_serialization>
    PARENT_SCOPE)
