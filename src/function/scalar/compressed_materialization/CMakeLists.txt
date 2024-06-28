add_library_unity(duckdb_func_compressed_materialization OBJECT
                  compress_integral.cpp compress_string.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_compressed_materialization>
    PARENT_SCOPE)
