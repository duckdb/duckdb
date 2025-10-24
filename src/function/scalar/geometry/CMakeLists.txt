add_library_unity(duckdb_func_geometry OBJECT geometry_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_geometry>
    PARENT_SCOPE)
