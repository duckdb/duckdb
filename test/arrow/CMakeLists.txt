add_library_unity(test_arrow_roundtrip OBJECT arrow_test_helper.cpp
                  arrow_roundtrip.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_arrow_roundtrip>
    PARENT_SCOPE)
