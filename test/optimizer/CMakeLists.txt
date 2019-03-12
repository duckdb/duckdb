
add_library(test_optimizer OBJECT test_arithmetic_simplification.cpp test_constant_folding.cpp test_distributivity_rule.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer> PARENT_SCOPE)
