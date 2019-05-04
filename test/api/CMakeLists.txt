
add_library(test_api OBJECT test_api.cpp test_results.cpp test_read_only.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_api> PARENT_SCOPE)
