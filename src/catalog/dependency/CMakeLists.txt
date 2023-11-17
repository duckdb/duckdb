add_library_unity(duckdb_catalog_dependency OBJECT dependency_list.cpp
                  dependency_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_catalog_dependency>
    PARENT_SCOPE)
