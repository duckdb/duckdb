
add_library(test_monetdb OBJECT test_leftjoin_bug.cpp test_updates_with_correlated_subselect.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_monetdb> PARENT_SCOPE)
