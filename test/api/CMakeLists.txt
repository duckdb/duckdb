
add_library(test_api OBJECT test_api.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_api> PARENT_SCOPE)
