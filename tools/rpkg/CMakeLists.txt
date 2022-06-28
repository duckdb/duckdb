include_directories(src)

execute_process(
  WORKING_DIRECTORY .
  COMMAND R RHOME
  OUTPUT_VARIABLE R_ROOT_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)

find_path(
  R_INCLUDE_DIR R.h
  HINTS ${R_ROOT_DIR}
  PATHS /usr/local/lib /usr/local/lib64 /usr/share /opt/homebrew/lib/
  PATH_SUFFIXES include R/include
  DOC "Path to file R.h")

find_library(
  R_LIBRARY_BASE R
  HINTS ${R_ROOT_DIR}/lib
  DOC "R library (example libR.a, libR.dylib, etc.).")

include_directories(${R_INCLUDE_DIR})
include_directories(../../)
include_directories(src/include)
include_directories(inst/include)

# this is used for clang-tidy checks and IDE support
add_library(
  duckdb_r_shlib SHARED
  src/database.cpp
  src/connection.cpp
  src/statement.cpp
  src/register.cpp
  src/scan.cpp
  src/utils.cpp
  src/altrep.cpp
  src/relational.cpp
  src/types.cpp)
target_link_libraries(duckdb_r_shlib duckdb ${DUCKDB_EXTRA_LINK_FLAGS}
                      ${R_LIBRARY_BASE})
