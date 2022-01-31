cmake_minimum_required(VERSION 2.8.12)

project(SubstraitExtension)

include_directories(include ../../third_party/substrait)

add_library(substrait_extension STATIC to_substrait.cpp ${DBGEN_OBJECT_FILES})
target_link_libraries(substrait_extension substrait)

build_loadable_extension(substrait substrait-extension.cpp
                         ${DBGEN_OBJECT_FILES})

install(
  TARGETS substrait_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
