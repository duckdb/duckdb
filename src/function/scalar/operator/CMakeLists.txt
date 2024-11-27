add_library_unity(duckdb_func_ops_main OBJECT add.cpp arithmetic.cpp
                  multiply.cpp subtract.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_ops_main>
    PARENT_SCOPE)
