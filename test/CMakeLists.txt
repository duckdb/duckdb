
include_directories(../third_party/catch)
include_directories(../third_party/dbgen/include)
include_directories(include)

add_subdirectory(common)
add_subdirectory(sql)
add_subdirectory(sqlite)
add_subdirectory(optimizer)


add_executable(test test.cpp)
target_link_libraries(test duckdb dbgen)

add_executable(unittest unittest.cpp ${ALL_OBJECT_FILES})
target_link_libraries(unittest duckdb dbgen)
