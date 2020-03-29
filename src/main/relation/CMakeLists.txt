add_library_unity(duckdb_main_relation
	OBJECT
	projection_relation.cpp
	table_relation.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_relation>
    PARENT_SCOPE)
