add_executable(arrow arrow.cpp)
find_package(Arrow REQUIRED)

include_directories(/usr/local/include/)

target_link_libraries(arrow duckdb arrow_shared)