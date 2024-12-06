add_library_unity(duckdb_sample OBJECT base_reservoir_sample.cpp
                  reservoir_sample.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_sample>
    PARENT_SCOPE)
