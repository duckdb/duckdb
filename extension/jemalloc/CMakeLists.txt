cmake_minimum_required(VERSION 2.8.12)

project(JEMallocExtension)

include_directories(include)
include_directories(jemalloc/include)


add_subdirectory(jemalloc)

set(JEMALLOC_EXTENSION_FILES jemalloc-extension.cpp ${JEMALLOC_OBJECT_FILES})

add_library(jemalloc_extension STATIC ${JEMALLOC_EXTENSION_FILES})

build_loadable_extension(jemalloc ${JEMALLOC_EXTENSION_FILES})

install(
  TARGETS jemalloc_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
