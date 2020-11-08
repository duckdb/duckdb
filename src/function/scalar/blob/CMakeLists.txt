add_library_unity(duckdb_func_blob OBJECT encode.cpp base64.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_blob>
    PARENT_SCOPE)
