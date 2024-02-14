add_library_unity(duckdb_csv_state_machine OBJECT csv_state_machine.cpp
                  csv_state_machine_cache.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_csv_state_machine>
    PARENT_SCOPE)
