if(${WASM_LOADABLE_EXTENSIONS})
  return()
endif()

cmake_minimum_required(VERSION 2.8.12)

project(HTTPFsExtension)

add_extension_definitions()

include_directories(include ../../third_party/httplib ../parquet/include)

add_library(httpfs_extension STATIC s3fs.cpp httpfs.cpp crypto.cpp
        httpfs_extension.cpp)
set(PARAMETERS "-warnings")
build_loadable_extension(httpfs ${PARAMETERS} s3fs.cpp httpfs.cpp crypto.cpp
        httpfs_extension.cpp)

if(MINGW)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()

find_package(OpenSSL REQUIRED)
target_link_libraries(httpfs_loadable_extension duckdb_mbedtls
                      ${OPENSSL_LIBRARIES})

if(MINGW)
  find_package(ZLIB)
  target_link_libraries(httpfs_loadable_extension ZLIB::ZLIB -lcrypt32)
endif()

install(
  TARGETS httpfs_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
