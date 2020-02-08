include_directories(../third_party/catch)
include_directories(../third_party/dbgen/include)
include_directories(../third_party/tpce-tool/include)
include_directories(../third_party/sqlite/include)
include_directories(../tools/dbtransfer/include)
include_directories(include)

add_subdirectory(api)
add_subdirectory(appender)
add_subdirectory(common)
add_subdirectory(helpers)
add_subdirectory(monetdb)
add_subdirectory(nested)
add_subdirectory(sql)
add_subdirectory(planner)
add_subdirectory(optimizer)
add_subdirectory(parser)

if(NOT WIN32)
  add_subdirectory(sqlserver)
  add_subdirectory(sakila)
  add_subdirectory(sqlite)
  add_subdirectory(sqlsmith)
  add_subdirectory(tpce)
  add_subdirectory(persistence)
  add_subdirectory(dbtransfer)
endif()

add_executable(unittest unittest.cpp ${ALL_OBJECT_FILES})

if(NOT WIN32)
  target_link_libraries(unittest
                        duckdb
                        dbgen
                        dsdgen
                        test_helpers
                        tpce
                        sqlite_transfer)
else()
  target_link_libraries(unittest duckdb_static test_helpers)
endif()
