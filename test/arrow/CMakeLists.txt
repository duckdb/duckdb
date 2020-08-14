add_executable(arrow arrow.cpp)
find_package(Arrow REQUIRED)

target_link_libraries(arrow duckdb arrow_shared)
