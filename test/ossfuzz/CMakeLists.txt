add_library_unity(test_ossfuzz OBJECT test_ossfuzz.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_ossfuzz>
    PARENT_SCOPE)
