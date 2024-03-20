add_definitions(-DSQLITE_OMIT_LOAD_EXTENSION=1)

include_directories(../sqlite3_api_wrapper/include)
if(NOT WIN32)
  add_subdirectory(linenoise)
  add_definitions(-DHAVE_LINENOISE=1)
  include_directories(../../third_party/utf8proc/include)
  include_directories(linenoise/include)
endif()
set(SHELL_SOURCES ${SHELL_SOURCES} shell.c)

option(STATIC_LIBCPP "Statically link CLI to libc++" FALSE)

add_executable(shell ${SHELL_SOURCES})
target_link_libraries(shell sqlite3_api_wrapper_static
                      ${DUCKDB_EXTRA_LINK_FLAGS})
link_threads(shell)
if(STATIC_LIBCPP)
  message("Statically linking CLI")
  target_link_libraries(shell -static-libstdc++ -static-libgcc)
endif()

if(NOT AMALGAMATION_BUILD AND NOT WIN32)
  target_link_libraries(shell duckdb_utf8proc)
endif()

function(ensure_variable_is_number INPUT_VERSION OUT_RESULT)
  if(NOT "${${INPUT_VERSION}}" MATCHES "^[0-9]+$")
    message(
      WARNING
        "VERSION PARAMETER ${INPUT_VERSION} \"${${INPUT_VERSION}}\" IS NOT A NUMBER - SETTING TO 0"
    )
    set(${OUT_RESULT}
        0
        PARENT_SCOPE)
  else()
    set(${OUT_RESULT}
        ${${INPUT_VERSION}}
        PARENT_SCOPE)
  endif()
endfunction()

if(WIN32 AND NOT MINGW)
  string(TIMESTAMP DUCKDB_COPYRIGHT_YEAR "%Y")
  ensure_variable_is_number(DUCKDB_MAJOR_VERSION RC_MAJOR_VERSION)
  ensure_variable_is_number(DUCKDB_MINOR_VERSION RC_MINOR_VERSION)
  ensure_variable_is_number(DUCKDB_PATCH_VERSION RC_PATCH_VERSION)
  ensure_variable_is_number(DUCKDB_DEV_ITERATION RC_DEV_ITERATION)

  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -D DUCKDB_VERSION=\"${DUCKDB_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -D DUCKDB_MAJOR_VERSION=\"${RC_MAJOR_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -D DUCKDB_MINOR_VERSION=\"${RC_MINOR_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -D DUCKDB_PATCH_VERSION=\"${RC_PATCH_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -D DUCKDB_DEV_ITERATION=\"${RC_DEV_ITERATION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -D DUCKDB_COPYRIGHT_YEAR=\"${DUCKDB_COPYRIGHT_YEAR}\"")
  target_sources(shell PRIVATE rc/duckdb.rc)
endif()

set_target_properties(shell PROPERTIES OUTPUT_NAME duckdb)
set_target_properties(shell PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                       ${PROJECT_BINARY_DIR})

install(TARGETS shell RUNTIME DESTINATION "${INSTALL_BIN_DIR}")
