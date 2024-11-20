add_library_unity(duckdb_csv_encode OBJECT csv_encoder.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_csv_encode>
    PARENT_SCOPE)
