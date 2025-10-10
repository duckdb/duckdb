add_library_unity(duckdb_sort OBJECT hashed_sort.cpp sort.cpp sorted_run.cpp
                  sorted_run_merger.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_sort>
    PARENT_SCOPE)
