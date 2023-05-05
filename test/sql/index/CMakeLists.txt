add_library_unity(test_index OBJECT test_art_index.cpp test_art_keys.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_index>
    PARENT_SCOPE)
