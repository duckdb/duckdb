include_directories(../third_party/catch)
include_directories(../third_party/sqlite/include)
include_directories(../test/include)
include_directories(include)

add_third_party(imdb)

add_subdirectory(micro)
list(FIND DUCKDB_EXTENSION_NAMES tpch _index)
if(${_index} GREATER -1)
  add_subdirectory(tpch)
endif()

add_extension_definitions()
set(BENCHMARK_ROOT_DIRECTORY
    ""
    CACHE
      STRING
      "The benchmark runner root directory (defaults to the project source directory)"
)
if(DEFINED BENCHMARK_ROOT_DIRECTORY AND NOT BENCHMARK_ROOT_DIRECTORY STREQUAL
                                        "")
  add_definitions(-DDUCKDB_ROOT_DIRECTORY="${BENCHMARK_ROOT_DIRECTORY}")
else()
  add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")
endif()

add_executable(benchmark_runner benchmark_runner.cpp interpreted_benchmark.cpp
                                ${BENCHMARK_OBJECT_FILES})

target_link_libraries(benchmark_runner duckdb_static imdb test_helpers)
link_extension_libraries(benchmark_runner "")
