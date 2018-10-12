
add_library(test_sql_catalog OBJECT schema.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_catalog> PARENT_SCOPE)
