add_library_unity(duckdb_peg_tokenizer OBJECT
        base_tokenizer.cpp
        parser_tokenizer.cpp)

set(AUTOCOMPLETE_EXTENSION_FILES
        ${AUTOCOMPLETE_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_peg_tokenizer>
        PARENT_SCOPE)
