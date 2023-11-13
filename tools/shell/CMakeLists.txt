add_definitions(-DSQLITE_OMIT_LOAD_EXTENSION=1)

set(SHELL_SOURCES shell.c)
if(NOT WIN32)
  add_definitions(-DHAVE_LINENOISE=1)
  set(SHELL_SOURCES ${SHELL_SOURCES} linenoise.cpp)
  include_directories(../../third_party/utf8proc/include)
endif()

option(STATIC_LIBCPP "Statically link CLI to libc++" FALSE)

include_directories(include)
include_directories(../sqlite3_api_wrapper/include)
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

if(WIN32)
  string(TIMESTAMP DUCKDB_COPYRIGHT_YEAR "%Y")
  set(CMAKE_RC_FLAGS "${CMAKE_RC_FLAGS} -dDUCKDB_VERSION=\"${DUCKDB_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -dDUCKDB_MAJOR_VERSION=\"${DUCKDB_MAJOR_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -dDUCKDB_MINOR_VERSION=\"${DUCKDB_MINOR_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -dDUCKDB_PATCH_VERSION=\"${DUCKDB_PATCH_VERSION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -dDUCKDB_DEV_ITERATION=\"${DUCKDB_DEV_ITERATION}\"")
  set(CMAKE_RC_FLAGS
      "${CMAKE_RC_FLAGS} -dDUCKDB_COPYRIGHT_YEAR=\"${DUCKDB_COPYRIGHT_YEAR}\"")
  target_sources(shell PRIVATE rc/duckdb.rc)
endif()

set_target_properties(shell PROPERTIES OUTPUT_NAME duckdb)
set_target_properties(shell PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                       ${PROJECT_BINARY_DIR})

install(TARGETS shell RUNTIME DESTINATION "${INSTALL_BIN_DIR}")
