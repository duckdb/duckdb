add_library_unity(duckdb_main_relation
	OBJECT
	filter_relation.cpp
	limit_relation.cpp
	projection_relation.cpp
	table_relation.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_relation>
    PARENT_SCOPE)
