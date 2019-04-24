add_library(test_persistence OBJECT test_persistence.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_persistence> PARENT_SCOPE)