add_library_unity(duckdb_func_generic
                  OBJECT
                  constant_or_null.cpp
                  least.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_generic>
    PARENT_SCOPE)
