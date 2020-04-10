add_library_unity(test_rigger OBJECT test_rigger.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_rigger>
    PARENT_SCOPE)
