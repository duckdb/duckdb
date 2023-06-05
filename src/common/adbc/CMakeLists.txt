add_library_unity(duckdb_adbc OBJECT adbc.cpp driver_manager.cpp)
add_subdirectory(nanoarrow)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_adbc>
    PARENT_SCOPE)
