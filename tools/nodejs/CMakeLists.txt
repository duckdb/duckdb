include_directories(src)
include_directories( /opt/homebrew/lib/node_modules/node-addon-api )
include_directories(/opt/homebrew/Cellar/node/17.4.0/include/node)

set(CMAKE_SHARED_LINKER_FLAGS "-bundle" "-undefined dynamic_lookup")
add_library(node_duckdb src/duckdb_node.cpp src/database.cpp src/connection.cpp
                        src/statement.cpp src/utils.cpp)
target_link_libraries(node_duckdb duckdb_static)
link_threads(node_duckdb)
