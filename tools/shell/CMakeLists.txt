find_library(READLINE_LIB readline)
find_library(EDITLINE_LIB editline)

set(HAVE_READLINE 0)
set(HAVE_EDITLINE 0)

if(EDITLINE_LIB)
  add_compile_options(-DHAVE_EDITLINE=1)
  set(LINE_LIB ${EDITLINE_LIB})
elseif(READLINE_LIB)
  add_compile_options(-DHAVE_READLINE=1)
  set(LINE_LIB ${READLINE_LIB})

endif()



add_compile_options(-DSQLITE_OMIT_LOAD_EXTENSION=1)

include_directories(include)
include_directories(../sqlite3_api_wrapper/include)

add_executable(shell shell.c)
target_link_libraries(shell ${LINE_LIB} sqlite3_api_wrapper_static)
set_target_properties(shell PROPERTIES OUTPUT_NAME duckdb)

set_target_properties(shell PROPERTIES
                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)
