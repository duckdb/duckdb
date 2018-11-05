
add_library(test_sql_function OBJECT substring.cpp table_function.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_function> PARENT_SCOPE)
