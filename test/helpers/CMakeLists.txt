
add_library(test_helpers OBJECT capi_helpers.cpp test_helpers.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_helpers> PARENT_SCOPE)
