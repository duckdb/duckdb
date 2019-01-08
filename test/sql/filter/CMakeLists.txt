
add_library(test_sql_filter OBJECT test_obsolete_filters.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_filter> PARENT_SCOPE)
