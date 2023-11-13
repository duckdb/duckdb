add_library_unity(test_tpch OBJECT test_tpch.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_tpch>
    PARENT_SCOPE)
