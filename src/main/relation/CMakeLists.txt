add_library_unity(duckdb_main_relation
	OBJECT
	create_view_relation.cpp
	distinct_relation.cpp
	filter_relation.cpp
	limit_relation.cpp
	order_relation.cpp
	projection_relation.cpp
	setop_relation.cpp
	table_relation.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_relation>
    PARENT_SCOPE)
