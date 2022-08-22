cmake_minimum_required(VERSION 2.8.12)

project(HashExtension)

include_directories(include)
include_directories(libs/xxHash)

set(HASH_SOURCES
    hash-extension.cpp
    libs/xxHash/xxhash.cpp)

add_library(hash_extension STATIC ${HASH_SOURCES})
set(INGORE_WARNINGS OFF)
build_loadable_extension(hash ${HASH_SOURCES} ${IGNORE_WARNINGS})

install(
  TARGETS hash_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
