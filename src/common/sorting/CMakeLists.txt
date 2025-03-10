add_library_unity(duckdb_sorting OBJECT sort.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_sorting>
    PARENT_SCOPE)
