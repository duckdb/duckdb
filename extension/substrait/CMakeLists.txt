cmake_minimum_required(VERSION 2.8.12)

set(CMAKE_CXX_STANDARD 17)
project(SubstraitExtension)

include_directories(include ../../third_party/substrait)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(SUBSTRAIT_SOURCES
    ../../third_party/substrait/substrait/capabilities.pb.cc
    ../../third_party/substrait/substrait/expression.pb.cc
    ../../third_party/substrait/substrait/function.pb.cc
    ../../third_party/substrait/substrait/parameterized_types.pb.cc
    ../../third_party/substrait/substrait/plan.pb.cc
    ../../third_party/substrait/substrait/relations.pb.cc
    ../../third_party/substrait/substrait/type.pb.cc
    ../../third_party/substrait/substrait/type_expressions.pb.cc
    ../../third_party/substrait/substrait/extensions/extensions.pb.cc)

add_library(substrait_extension STATIC to_substrait.cpp substrait-extension.cpp
                                       ${SUBSTRAIT_SOURCES})

build_loadable_extension(substrait substrait-extension.cpp)
target_link_libraries(substrait_extension ${Protobuf_LIBRARIES})
install(
  TARGETS substrait_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
