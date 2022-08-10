add_library_unity(duckdb_func_cast OBJECT cast_function_set.cpp default_casts.cpp enum_casts.cpp list_casts.cpp numeric_casts.cpp struct_cast.cpp time_casts.cpp uuid_casts.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_cast>
    PARENT_SCOPE)
