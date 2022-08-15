cmake_minimum_required(VERSION 2.8.12)

project(HTTPFsExtension)

add_extension_definitions()

include_directories(include ../.. ../../third_party/httplib
                    ../../third_party/picohash ../parquet/include)

add_library(httpfs_extension STATIC s3fs.cpp httpfs.cpp crypto.cpp
                                    httpfs-extension.cpp)
set(INGORE_WARNINGS OFF)
build_loadable_extension(httpfs s3fs.cpp httpfs.cpp crypto.cpp
                         httpfs-extension.cpp ${IGNORE_WARNINGS})

find_package(OpenSSL REQUIRED)
target_link_libraries(httpfs_loadable_extension ${OPENSSL_LIBRARIES})
install(
  TARGETS httpfs_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
