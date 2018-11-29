
add_library(test_index OBJECT test_index.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_index> PARENT_SCOPE)
