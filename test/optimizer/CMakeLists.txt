
add_library(test_optimizer OBJECT test_constant_folding.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer> PARENT_SCOPE)
