add_library_unity(
  duckdb_catalog_entries
  OBJECT
  copy_function_catalog_entry.cpp
  enum_catalog_entry.cpp
  index_catalog_entry.cpp
  macro_catalog_entry.cpp
  pragma_function_catalog_entry.cpp
  schema_catalog_entry.cpp
  sequence_catalog_entry.cpp
  table_catalog_entry.cpp
  table_function_catalog_entry.cpp
  view_catalog_entry.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_catalog_entries>
    PARENT_SCOPE)
