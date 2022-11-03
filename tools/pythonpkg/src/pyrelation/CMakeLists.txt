add_library(python_relation OBJECT initialize.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_relation>
    PARENT_SCOPE)
