
add_library(test_sql_pragma OBJECT pragma.cpp table_info.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_pragma> PARENT_SCOPE)
