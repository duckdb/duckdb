add_library_unity(duckdb_table_function OBJECT pragma_table_info.cpp sqlite_master.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_table_function> PARENT_SCOPE)
