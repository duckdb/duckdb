
add_library(test_sql_simple OBJECT case.cpp distinct.cpp expressions.cpp groupby.cpp inserts.cpp join.cpp manycolumns.cpp null_handling.cpp nullboolean.cpp orderby.cpp overflow_handling.cpp scalar_projections.cpp simple_projections.cpp subquery.cpp setops.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_simple> PARENT_SCOPE)
