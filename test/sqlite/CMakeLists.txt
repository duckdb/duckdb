
include_directories(../../src/include)

add_executable(sqllogictest md5.c sqllogictest.c)
target_link_libraries(sqllogictest duckdb)
