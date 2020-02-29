add_library_unity(test_planner OBJECT test_projection_binding.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_planner>
    PARENT_SCOPE)
