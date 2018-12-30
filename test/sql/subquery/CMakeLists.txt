
add_library(test_subquery OBJECT test_correlated_subquery.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_subquery> PARENT_SCOPE)
