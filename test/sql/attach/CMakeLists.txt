add_library_unity(test_attach OBJECT test_attach.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_attach>
    PARENT_SCOPE)
