add_library_unity(test_sql_storage_catalog
  OBJECT
  test_constraints.cpp
  test_droptable.cpp
  test_prepared_storage.cpp
  test_storage_defaults.cpp
  test_storage_sequences.cpp
  test_store_alter.cpp
  test_store_rename_table.cpp
  test_temporary.cpp
  test_views.cpp)

set(ALL_OBJECT_FILES
${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_storage_catalog>
PARENT_SCOPE)
