cmake_minimum_required(VERSION 2.8.12)

set(CMAKE_CXX_STANDARD 17)
project(SubstraitExtension)

include_directories(include ../../third_party/substrait)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
add_library(substrait_extension STATIC to_substrait.cpp substrait-extension.cpp)

build_loadable_extension(substrait substrait-extension.cpp)
target_link_libraries(substrait_loadable_extension substrait_og
                      ${Protobuf_LIBRARIES})
install(
  TARGETS substrait_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
