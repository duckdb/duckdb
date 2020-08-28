add_library_unity(duckdb_func_generic
                  OBJECT
                  least.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_generic>
    PARENT_SCOPE)
