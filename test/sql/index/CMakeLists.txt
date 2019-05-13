
add_library_unity(test_index OBJECT test_index.cpp test_concurrent_index.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_index> PARENT_SCOPE)
