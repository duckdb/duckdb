add_library_unity(
    traceflags
    OBJECT
    traceflags.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:traceflags>
    PARENT_SCOPE)