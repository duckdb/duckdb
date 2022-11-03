add_library(python_common OBJECT exceptions.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_common>
    PARENT_SCOPE)
