
add_library(test_sql_join OBJECT test_left_outer_join.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_join> PARENT_SCOPE)
