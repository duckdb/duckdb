add_library_unity(
  duckdb_table_func_system
  OBJECT
  duckdb_schemas.cpp
  pragma_collations.cpp
  pragma_database_list.cpp
  pragma_database_size.cpp
  pragma_functions.cpp
  pragma_storage_info.cpp
  pragma_table_info.cpp
  sqlite_master.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_table_func_system>
    PARENT_SCOPE)
