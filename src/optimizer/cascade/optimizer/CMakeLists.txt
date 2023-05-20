add_library_unity(
    optimizer
    OBJECT
    COptimizerConfig.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:optimizer>
    PARENT_SCOPE)