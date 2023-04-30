add_library_unity(duckdb_func_random OBJECT random.cpp setseed.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_random>
    PARENT_SCOPE)
