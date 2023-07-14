cmake_minimum_required(VERSION 2.8.12)

project(JemallocExtension)

include_directories(include)
include_directories(jemalloc/include)

add_subdirectory(jemalloc)

set(JEMALLOC_EXTENSION_FILES jemalloc_extension.cpp ${JEMALLOC_OBJECT_FILES})

build_static_extension(jemalloc ${JEMALLOC_EXTENSION_FILES})
# we do not do build_loadable_extension here because jemalloc is static-only

install(
  TARGETS jemalloc_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
