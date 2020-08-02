add_library_unity(duckdb_transformer_constraint OBJECT transform_constraint.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_transformer_constraint>
    PARENT_SCOPE)
