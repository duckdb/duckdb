add_library_unity(duckdb_parser_peg_keyword_helper OBJECT
                  duckdb_keyword_helper.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser_peg_keyword_helper>
    PARENT_SCOPE)
