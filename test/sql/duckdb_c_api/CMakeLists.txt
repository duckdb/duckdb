
add_library(test_sql_capi OBJECT join.cpp groupby.cpp null_handling.cpp orderby.cpp overflow_handling.cpp starting_database.cpp simple_projections.cpp scalar_projections.cpp tpch.cpp subquery.cpp copy.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi> PARENT_SCOPE)
