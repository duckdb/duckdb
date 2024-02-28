include_directories(include)

add_library(duckdb_linenoise OBJECT highlighting.cpp history.cpp linenoise.cpp
                                    linenoise-c.cpp rendering.cpp terminal.cpp)
set(SHELL_SOURCES
    ${SHELL_SOURCES} $<TARGET_OBJECTS:duckdb_linenoise>
    PARENT_SCOPE)
