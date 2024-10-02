add_library_unity(
  duckdb_operator_persistent
  OBJECT
  csv_rejects_table.cpp
  physical_batch_copy_to_file.cpp
  physical_batch_insert.cpp
  physical_copy_database.cpp
  physical_copy_to_file.cpp
  physical_delete.cpp
  physical_export.cpp
  physical_insert.cpp
  physical_update.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_persistent>
    PARENT_SCOPE)
