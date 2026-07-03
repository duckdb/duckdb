add_library_unity(test_ossfuzz OBJECT test_ossfuzz.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_ossfuzz>
    PARENT_SCOPE)
