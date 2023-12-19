add_library_unity(duckdb_operator_csv_scanner_aux OBJECT base_scanner.cpp
                  dialect_scanner.cpp type_scanner.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_csv_scanner_aux>
    PARENT_SCOPE)
