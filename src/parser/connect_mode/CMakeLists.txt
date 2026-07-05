add_library_unity(duckdb_parser_connect_mode OBJECT connect_mode_parser.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser_connect_mode>
    PARENT_SCOPE)
