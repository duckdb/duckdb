add_library(duckdb_operator_persistent OBJECT physical_copy.cpp physical_delete.cpp physical_insert.cpp physical_update.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_persistent> PARENT_SCOPE)
