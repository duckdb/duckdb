add_library_unity(duckdb_func_math
                  OBJECT
                  numeric.cpp
                  random.cpp
                  setseed.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_math>
    PARENT_SCOPE)
