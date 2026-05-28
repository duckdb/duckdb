include_directories(include)

add_subdirectory(linenoise)
add_definitions(-DHAVE_LINENOISE=1)
include_directories(../../third_party/utf8proc/include)
include_directories(linenoise/include)
add_definitions(-DUSE_DUCKDB_SHELL_WRAPPER)

include_directories(../../third_party/utf8proc/include)

set(SHELL_SOURCES
    ${SHELL_SOURCES}
    shell_command_line_option.cpp
    shell_extension.cpp
    shell.cpp
    shell_helpers.cpp
    shell_metadata_command.cpp
    shell_prompt.cpp
    shell_renderer.cpp
    shell_highlight.cpp
    shell_progress_bar.cpp
    shell_render_table_metadata.cpp
    shell_windows.cpp)

add_library(duckdb_shell_static STATIC ${SHELL_SOURCES})

target_link_libraries(duckdb_shell_static duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
link_threads(duckdb_shell_static "")

link_extension_libraries(duckdb_shell_static "")

if(NOT AMALGAMATION_BUILD AND NOT WIN32)
  target_link_libraries(duckdb_shell_static duckdb_utf8proc)
endif()
