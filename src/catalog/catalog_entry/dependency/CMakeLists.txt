add_library_unity(duckdb_catalog_entries_dependency OBJECT dependency_entry.cpp
                  dependency_subject_entry.cpp dependency_dependent_entry.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_catalog_entries_dependency>
    PARENT_SCOPE)
