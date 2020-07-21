add_library_unity(test_sqlsmith OBJECT test_sqlsmith.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sqlsmith>
    PARENT_SCOPE)
