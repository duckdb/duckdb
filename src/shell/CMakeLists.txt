file(GLOB SHELL_SRC_FILES *.c sqlite3.h)
find_library(READLINE_LIB readline)
find_library(EDITLINE_LIB editline)

set(HAVE_READLINE 0)
set(HAVE_EDITLINE 0)

if(EDITLINE_LIB)
    set(HAVE_EDITLINE 1)
    set(LINE_LIB ${EDITLINE_LIB})
elseif(READLINE_LIB)
    set(HAVE_READLINE 1)
    set(LINE_LIB ${READLINE_LIB})
endif()

execute_process(
  COMMAND git log -1 --format=%h
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_COMMIT_HASH
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

configure_file (
        "./shell_config.h.in"
        "${CMAKE_BINARY_DIR}/src/shell/shell_config.h"
)
include_directories(${CMAKE_BINARY_DIR}/src/shell)

#add_library(duckdb_shell OBJECT ${SHELL_SRC_FILES})
set(SHELL_OBJECT_FILES ${SHELL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_shell>)
add_executable(shell ${SHELL_SRC_FILES} ${ALL_OBJECT_FILES})
target_link_libraries(shell ${LINE_LIB} pg_query Threads::Threads)