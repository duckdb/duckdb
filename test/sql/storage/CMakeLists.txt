
add_library(test_sql_storage OBJECT storage.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_storage> PARENT_SCOPE)
