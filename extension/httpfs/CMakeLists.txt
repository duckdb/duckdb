cmake_minimum_required(VERSION 2.8.12)

project(HTTPFsExtension)

add_extension_definitions()

include_directories(include ../.. ../../third_party/httplib ../../third_party/picohash ../parquet/include)

add_library(
  httpfs_extension STATIC
 s3fs.cpp crypto.cpp httpfs-extension.cpp)


add_executable(test_s3_parquet_read test.cpp)


target_link_libraries(test_s3_parquet_read duckdb_static httpfs_extension ${LINK_EXTRA})

link_extension_libraries(test_s3_parquet_read)
