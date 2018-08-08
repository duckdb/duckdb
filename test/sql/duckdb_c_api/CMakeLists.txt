
add_library(test_sql_capi OBJECT groupby.cpp null_handling.cpp orderby.cpp overflow_handling.cpp starting_database.cpp simple_projections.cpp scalar_projections.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi> PARENT_SCOPE)
