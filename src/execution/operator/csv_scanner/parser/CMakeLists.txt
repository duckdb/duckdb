add_library_unity(duckdb_operator_csv_parser OBJECT scanner_boundary.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_csv_parser>
    PARENT_SCOPE)
