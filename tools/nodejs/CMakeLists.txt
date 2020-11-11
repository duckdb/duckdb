include_directories(src)
include_directories(/usr/local/lib/node_modules/node-addon-api/)
include_directories(/usr/local/Cellar/node/15.0.1/include/node/)

set(CMAKE_SHARED_LINKER_FLAGS "-bundle" "-undefined dynamic_lookup")
add_library(node_duckdb src/duckdb_node.cpp src/database.cpp src/connection.cpp src/statement.cpp src/utils.cpp)
target_link_libraries(node_duckdb duckdb_static Threads::Threads)
