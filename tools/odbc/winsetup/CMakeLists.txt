project(odbc_install)

find_package(ODBC REQUIRED)

add_executable(odbc_install install.c)
target_link_libraries(
  odbc_install
  $<$<PLATFORM_ID:Windows>:odbc32>
  $<$<PLATFORM_ID:Windows>:odbccp32>
  $<$<PLATFORM_ID:Windows>:shlwapi>
  $<$<PLATFORM_ID:Windows>:user32>
  $<$<PLATFORM_ID:Windows>:legacy_stdio_definitions>)

add_library(duckdb_odbc_setup SHARED setup.c setup.rc setup.def resource.h)

target_link_libraries(
  duckdb_odbc_setup $<$<PLATFORM_ID:Windows>:odbc32>
  $<$<PLATFORM_ID:Windows>:odbccp32>
  $<$<PLATFORM_ID:Windows>:legacy_stdio_definitions>)
