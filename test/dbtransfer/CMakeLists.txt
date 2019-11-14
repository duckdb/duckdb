add_library_unity(test_dbtransfer OBJECT test_dbtransfer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_dbtransfer>
    PARENT_SCOPE)
