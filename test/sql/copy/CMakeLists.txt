
add_library(test_sql_copy OBJECT copy.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_copy> PARENT_SCOPE)
