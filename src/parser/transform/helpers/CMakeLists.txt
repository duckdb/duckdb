
add_library(duckdb_transformer_helpers OBJECT transform_alias.cpp transform_cte.cpp transform_groupby.cpp transform_orderby.cpp transform_typename.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_transformer_helpers> PARENT_SCOPE)
