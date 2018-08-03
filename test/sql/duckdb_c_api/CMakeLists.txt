
add_library(test_sql_capi OBJECT creation_statements.cpp starting_database.cpp simple_projections.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi> PARENT_SCOPE)
