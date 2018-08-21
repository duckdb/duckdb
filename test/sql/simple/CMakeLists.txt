
add_library(test_sql_simple OBJECT case.cpp expressions.cpp nullboolean.cpp groupby.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_simple> PARENT_SCOPE)
