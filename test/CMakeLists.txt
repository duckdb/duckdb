
include_directories(../third_party/catch)

add_subdirectory(common)

add_executable(test test.cpp)
target_link_libraries(test duckdb)

add_executable(microbenchmark microbenchmark.cpp ${ALL_OBJECT_FILES})
target_link_libraries(microbenchmark duckdb)
