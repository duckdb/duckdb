
add_library(test_sqlsmith OBJECT sqlsmith_test.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sqlsmith> PARENT_SCOPE)
