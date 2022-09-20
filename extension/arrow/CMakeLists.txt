cmake_minimum_required(VERSION 2.8.12)

project(ArrowExtension)

add_extension_definitions()

include_directories(include ../.. ../../third_party/httplib
                    ../../third_party/picohash ../parquet/include)

add_library(arrow_extension STATIC arrow-extension.cpp)
set(INGORE_WARNINGS OFF)
build_loadable_extension(arrow arrow-extension.cpp ${IGNORE_WARNINGS})

# set arrow path
set(ARROW_PATH /opt/homebrew/Cellar/apache-arrow/9.0.0_1)
set(ARROW_INCLUDE /opt/homebrew/Cellar/apache-arrow/9.0.0_1/include)
set(Lz4_SOURCE /opt/homebrew/Cellar/lz4/1.9.4)

find_package(Arrow REQUIRED)
target_link_libraries(arrow_loadable_extension arrow_shared)
target_link_libraries(arrow_extension arrow_shared)

install(
  TARGETS arrow_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
