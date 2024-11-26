
add_library_unity(
  duckdb_logging
  OBJECT
  logger.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logging>
    PARENT_SCOPE)
