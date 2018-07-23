
add_library(test_common OBJECT test_date.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_common> PARENT_SCOPE)
