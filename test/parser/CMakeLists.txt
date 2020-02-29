add_library_unity(test_parser OBJECT test_parser.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_parser>
    PARENT_SCOPE)
