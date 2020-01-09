find_library(READLINE_LIB readline)
find_library(EDITLINE_LIB editline)

set(HAVE_READLINE 0)
set(HAVE_EDITLINE 0)

if(EDITLINE_LIB)
  add_compile_definitions(HAVE_EDITLINE=1)
  set(LINE_LIB ${EDITLINE_LIB})
elseif(READLINE_LIB)
  add_compile_definitions(HAVE_READLINE=1)
  set(LINE_LIB ${READLINE_LIB})

endif()


add_compile_definitions(GIT_COMMIT_HASH=${GIT_COMMIT_HASH})


include_directories(include)
include_directories(../sqlite3_api_wrapper/include)

add_executable(shell shell.c)
target_link_libraries(shell ${LINE_LIB} sqlite3_api_wrapper)
