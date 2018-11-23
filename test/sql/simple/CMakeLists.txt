
add_library(test_sql_simple OBJECT case.cpp column_names.cpp conjunctions.cpp cte.cpp distinct.cpp expressions.cpp groupby.cpp having.cpp inserts.cpp join.cpp like.cpp manycolumns.cpp null_handling.cpp nullboolean.cpp orderby.cpp overflow_handling.cpp scalar_projections.cpp simple_projections.cpp subquery.cpp setops.cpp table_subquery.cpp update.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_simple> PARENT_SCOPE)
