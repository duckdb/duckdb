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
add_subdirectory(sql)
add_subdirectory(planner)
add_subdirectory(optimizer)

if(NOT WIN32 AND NOT SUN)
  add_subdirectory(sqlite)
  if (${BUILD_SQLSMITH})
    add_subdirectory(sqlsmith)
  endif()
  if (${BUILD_TPCE})
    add_subdirectory(tpce)
  endif()
  add_subdirectory(persistence)
  add_subdirectory(dbtransfer)
  add_subdirectory(nested)
endif()

add_executable(unittest unittest.cpp ${ALL_OBJECT_FILES})

if(NOT WIN32 AND NOT SUN)
  target_link_libraries(unittest
                        duckdb
                        dbgen
                        dsdgen
                        test_helpers
                        sqlite_transfer
                        )
  if (${BUILD_TPCE})
    target_link_libraries(unittest tpce)
  endif()
else()
  target_link_libraries(unittest duckdb_static test_helpers)
endif()

if(${BUILD_PARQUET_EXTENSION})
    target_link_libraries(unittest parquet_extension)
endif()

if(${BUILD_ICU_EXTENSION})
    target_link_libraries(unittest icu_extension)
endif()
