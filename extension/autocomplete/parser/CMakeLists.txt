add_library_unity(duckdb_peg_parser OBJECT peg_parser.cpp)
set(AUTOCOMPLETE_EXTENSION_FILES
    ${AUTOCOMPLETE_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_peg_parser>
    PARENT_SCOPE)
