add_library_unity(duckdb_peg_parser OBJECT peg_parser.cpp
                  peg_parser_override.cpp)
set(AUTOCOMPLETE_EXTENSION_FILES
    ${AUTOCOMPLETE_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_peg_parser>
    PARENT_SCOPE)
