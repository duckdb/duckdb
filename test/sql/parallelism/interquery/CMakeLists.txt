add_library_unity(
  test_sql_interquery_parallelism
  OBJECT
  concurrent_attach_detach.cpp
  concurrent_checkpoint.cpp
  test_concurrent_prepared.cpp
  test_default_catalog.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_interquery_parallelism>
    PARENT_SCOPE)
