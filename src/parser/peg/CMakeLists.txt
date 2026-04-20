add_subdirectory(tokenizer)
add_subdirectory(transformer)

add_library_unity(
        duckdb_parser_peg
        OBJECT
        autocomplete_core.cpp
        keyword_helper.cpp
        keyword_map.cpp
        peg_parser.cpp
        sql_formatter.cpp
        )
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser_peg>
        PARENT_SCOPE)
