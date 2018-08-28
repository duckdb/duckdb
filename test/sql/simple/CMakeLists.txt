
add_library(test_sql_simple OBJECT case.cpp distinct.cpp expressions.cpp groupby.cpp join.cpp null_handling.cpp nullboolean.cpp orderby.cpp overflow_handling.cpp scalar_projections.cpp subquery.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_simple> PARENT_SCOPE)
