
add_library(test_sql_constraints OBJECT notnull.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_constraints> PARENT_SCOPE)
