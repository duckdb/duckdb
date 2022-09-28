cmake_minimum_required(VERSION 2.8.12)

project(ArrowExtension)

add_extension_definitions()

include_directories(include ../.. ../../third_party/httplib
                    ../../third_party/picohash ../parquet/include)

add_library(arrow_extension STATIC arrow-extension.cpp arrow_stream_buffer.cpp)
set(INGORE_WARNINGS OFF)
build_loadable_extension(arrow arrow-extension.cpp arrow_stream_buffer.cpp ${IGNORE_WARNINGS})

find_package(Arrow REQUIRED)
target_link_libraries(arrow_loadable_extension arrow_shared)
target_link_libraries(arrow_extension arrow_shared)

install(
  TARGETS arrow_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
