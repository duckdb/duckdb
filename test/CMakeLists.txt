include_directories(../third_party/catch)
include_directories(../third_party/dbgen/include)
include_directories(../third_party/tpce-tool/include)
include_directories(../third_party/sqlite/include)
include_directories(include)

add_subdirectory(api)
add_subdirectory(appender)
add_subdirectory(common)
add_subdirectory(helpers)
add_subdirectory(monetdb)
add_subdirectory(sql)
add_subdirectory(optimizer)



add_executable(unittest unittest.cpp ${ALL_OBJECT_FILES})

target_link_libraries(unittest duckdb_static test_helpers)

