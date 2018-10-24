
add_library(test_sql_date OBJECT date.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_date> PARENT_SCOPE)
