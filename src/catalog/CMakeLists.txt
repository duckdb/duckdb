add_library(duckdb_catalog OBJECT catalog.cpp catalog_set.cpp schema_catalog.cpp table_catalog.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_catalog> PARENT_SCOPE)
