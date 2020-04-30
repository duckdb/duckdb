add_definitions(-DSQLITE_OMIT_LOAD_EXTENSION=1)

add_definitions(-DDUCKDB_SOURCE_ID="\""${GIT_COMMIT_HASH}"\"")

set(SHELL_SOURCES shell.c)
if(NOT WIN32)
  add_definitions(-DHAVE_LINENOISE=1)
  set(SHELL_SOURCES ${SHELL_SOURCES} linenoise.c)
  include_directories(../../third_party/third_party/utf8proc/include)
endif()

include_directories(include)
include_directories(../sqlite3_api_wrapper/include)
add_executable(shell ${SHELL_SOURCES})
target_link_libraries(shell sqlite3_api_wrapper_static)
if(NOT AMALGAMATION_BUILD AND NOT WIN32)
  target_link_libraries(shell utf8proc)
endif()
set_target_properties(shell PROPERTIES OUTPUT_NAME duckdb_cli)
set_target_properties(shell
                      PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
