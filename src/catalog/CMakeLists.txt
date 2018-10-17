
add_subdirectory(catalog_entry)

add_library(duckdb_catalog OBJECT catalog.cpp catalog_set.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_catalog> PARENT_SCOPE)
