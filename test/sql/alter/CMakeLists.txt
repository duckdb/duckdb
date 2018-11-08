
add_library(test_sql_alter OBJECT alter.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_alter> PARENT_SCOPE)
