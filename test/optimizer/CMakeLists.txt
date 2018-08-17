
add_library(test_optimizer OBJECT test_constant_folding.cpp test_subquery_rewrite.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer> PARENT_SCOPE)
