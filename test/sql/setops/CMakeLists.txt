add_library(test_sql_setops OBJECT test_joins_under_setops.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_setops> PARENT_SCOPE)
