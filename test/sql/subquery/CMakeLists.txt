
add_library(test_subquery OBJECT test_correlated_subquery.cpp test_subquery.cpp test_lateral.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_subquery> PARENT_SCOPE)
