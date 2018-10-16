
include_directories(include)
add_executable(sqlsmith duckdb.cc dump.cc expr.cc grammar.cc impedance.cc log.cc prod.cc random.cc relmodel.cc schema.cc sqlsmith.cc)
target_link_libraries(sqlsmith sqlite3_api_wrapper)
