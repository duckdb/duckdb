# ----------------------------------------------------
# Find Node headers

find_path(
  NODE_ROOT_DIR
  NAMES node.h src/node.h
  PATH_SUFFIXES
    node
    node4
    node5
    node6
    node7
    node8
    nodejs
  PATHS /usr/include /usr/local/include)

if(NODE_ROOT_DIR)
  macro(add_node_include_dir DIR)
    if(IS_DIRECTORY ${DIR} AND NOT ${DIR} STREQUAL "/usr/include")
      set(NODE_INCLUDE_DIRS ${NODE_INCLUDE_DIRS} ${DIR})
    endif()
  endmacro()
  add_node_include_dir(${NODE_ROOT_DIR})
  add_node_include_dir(${NODE_ROOT_DIR}/deps/uv/include)
  add_node_include_dir(${NODE_ROOT_DIR}/deps/v8/include)
  add_node_include_dir(${NODE_ROOT_DIR}/include/deps/v8/include)
  add_node_include_dir(${NODE_ROOT_DIR}/include/node)
  add_node_include_dir(${NODE_ROOT_DIR}/include/src)
  add_node_include_dir(${NODE_ROOT_DIR}/src)
else()
  unset(NODE_INCLUDE_DIRS)
  message(ERROR "Failed to find node headers")
endif()

find_path(
  NAPI_INCLUDE_DIR
  NAMES napi.h
  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/node_modules/node-addon-api)

if(NAPI_INCLUDE_DIR)
  add_node_include_dir(${NAPI_INCLUDE_DIR})
else()
  unset(NAPI_INCLUDE_DIR)
  message(ERROR "Failed to find node api headers")
endif()

add_library(napi INTERFACE)
target_include_directories(napi INTERFACE ${NODE_INCLUDE_DIRS})

# ----------------------------------------------------
# Include headers

message(STATUS "NODE_INCLUDE_DIRS=${NODE_INCLUDE_DIRS}")
include_directories(src)
include_directories(${NODE_INCLUDE_DIRS})

# ----------------------------------------------------
# Build library

set(CMAKE_SHARED_LINKER_FLAGS "-bundle" "-undefined dynamic_lookup")
add_library(node_duckdb src/duckdb_node.cpp src/database.cpp src/data_chunk.cpp
                        src/connection.cpp src/statement.cpp src/utils.cpp)
target_link_libraries(node_duckdb duckdb_static napi)
link_threads(node_duckdb)
