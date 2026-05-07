add_library_unity(test_index OBJECT test_art_index.cpp test_art_keys.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_index>
    PARENT_SCOPE)
