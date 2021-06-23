set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

include(ExternalProject)

ExternalProject_Add(
  arrow_ep
  GIT_REPOSITORY "https://github.com/apache/arrow"
  GIT_TAG f959141ece4d660bce5f7fa545befc0116a7db79
  PREFIX "${CMAKE_BINARY_DIR}/third_party/arrow"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow/install"
  BUILD_BYPRODUCTS
    <INSTALL_DIR>/lib/libarrow.a
    <INSTALL_DIR>/lib/libarrow_bundled_dependencies.a
    <INSTALL_DIR>/lib/libparquet.a
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_C_COMPILER_LAUNCHER=${CMAKE_C_COMPILER_LAUNCHER}
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/arrow/install
    -DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH}
    -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE} -DARROW_ALTIVEC=OFF
    -DARROW_USE_CCACHE=ON -DARROW_BOOST_USE_SHARED=OFF -DARROW_BUILD_SHARED=OFF
    -DARROW_BUILD_STATIC=ON -DARROW_BUILD_UTILITIES=OFF -DARROW_COMPUTE=OFF
    -DARROW_DATASET=OFF -DARROW_FLIGHT=OFF -DARROW_GFLAGS_USE_SHARED=OFF
    -DARROW_HDFS=OFF -DARROW_IPC=ON -DARROW_JSON=ON -DARROW_CSV=OFF
    -DARROW_JEMALLOC=OFF -DARROW_ORC=OFF -DARROW_PARQUET=ON
    -DARROW_PROTOBUF_USE_SHARED=OFF -DARROW_USE_GLOG=OFF -DARROW_SIMD_LEVEL=NONE
    -DARROW_RUNTIME_SIMD_LEVEL=NONE -DARROW_WITH_BROTLI=OFF -DARROW_WITH_LZ4=OFF
    -DARROW_WITH_PROTOBUF=OFF -DARROW_WITH_RAPIDJSON=OFF -DARROW_WITH_SNAPPY=ON
    -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DBOOST_SOURCE=AUTO
    <SOURCE_DIR>/cpp
  UPDATE_COMMAND "")

ExternalProject_Get_Property(arrow_ep install_dir)

set(install_arrow ${install_dir})

ExternalProject_Add(
  RE2
  GIT_REPOSITORY "https://github.com/google/re2"
  GIT_TAG cd026c5bfe9f9d713d9302ede331fa2696018b26
  PREFIX "${CMAKE_BINARY_DIR}/third_party/re2"
  BINARY_DIR "${CMAKE_BINARY_DIR}/third_party/re2/src/RE2"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/re2/install"
  BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libre2.a
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/re2/install
  UPDATE_COMMAND "")

ExternalProject_Get_Property(RE2 install_dir)

set(install_re2 ${install_dir})

ExternalProject_Add(
  UTF8
  GIT_REPOSITORY "https://github.com/pdet/utf8proc.git"
  DOWNLOAD_DIR "{CMAKE_BINARY_DIR}/third_party/utf8"
  PREFIX "${CMAKE_BINARY_DIR}/third_party/utf8"
  BINARY_DIR "${CMAKE_BINARY_DIR}/third_party/utf8/src/UTF8"
  SOURCE_DIR "${CMAKE_BINARY_DIR}/third_party/utf8/src/UTF8"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/utf8/install"
  BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libutf8proc.a
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/utf8/install
  UPDATE_COMMAND "")

ExternalProject_Get_Property(UTF8 install_dir)

set(install_utf8 ${install_dir})

ExternalProject_Add(
  ZLIB
  GIT_REPOSITORY "https://github.com/madler/zlib"
  DOWNLOAD_DIR "{CMAKE_BINARY_DIR}/third_party/zlib"
  PREFIX "${CMAKE_BINARY_DIR}/third_party/zlib"
  BINARY_DIR "${CMAKE_BINARY_DIR}/third_party/zlib/src/ZLIB"
  SOURCE_DIR "${CMAKE_BINARY_DIR}/third_party/zlib/src/ZLIB"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/zlib/install"
  BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libz.a
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/zlib/install
  UPDATE_COMMAND "")

ExternalProject_Get_Property(ZLIB install_dir)

set(install_zlib ${install_dir})

ExternalProject_Add(
  SNAPPY
  GIT_REPOSITORY "https://github.com/google/snappy"
  DOWNLOAD_DIR "{CMAKE_BINARY_DIR}/third_party/snappy"
  PREFIX "${CMAKE_BINARY_DIR}/third_party/snappy"
  BINARY_DIR "${CMAKE_BINARY_DIR}/third_party/snappy/src/SNAPPY"
  SOURCE_DIR "${CMAKE_BINARY_DIR}/third_party/snappy/src/SNAPPY"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/snappy/install"
  BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libsnappy.a
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/snappy/install
  UPDATE_COMMAND "")

ExternalProject_Get_Property(SNAPPY install_dir)

set(install_snappy ${install_dir})

ExternalProject_Add(
  ZSTD
  GIT_REPOSITORY "https://github.com/facebook/zstd"
  GIT_TAG a488ba114ec17ea1054b9057c26a046fc122b3b6
  DOWNLOAD_DIR "{CMAKE_BINARY_DIR}/third_party/zstd"
  PREFIX "${CMAKE_BINARY_DIR}/third_party/zstd"
  BINARY_DIR "${CMAKE_BINARY_DIR}/third_party/zstd/src/ZSTD/build/cmake"
  SOURCE_DIR "${CMAKE_BINARY_DIR}/third_party/zstd/src/ZSTD"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/zstd/install"
  BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libzstd.a
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/zstd/install
  UPDATE_COMMAND "")

ExternalProject_Get_Property(ZSTD install_dir)

set(install_zstd ${install_dir})

include_directories(${install_arrow}/include)
include_directories(../../third_party/catch)

set(TEST_ARROW_OBJECTS abi_test.cpp parquet_test.cpp arrow_check.cpp)

add_library_unity(test_arrow OBJECT ${TEST_ARROW_OBJECTS})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_arrow>
    PARENT_SCOPE)

add_dependencies(
  test_arrow
  RE2
  arrow_ep
  UTF8
  ZLIB
  SNAPPY
  ZSTD)

target_link_libraries(
  test_arrow
  ${install_arrow}/lib/libarrow_bundled_dependencies.a
  ${install_arrow}/lib/libparquet.a
  ${install_arrow}/lib/libarrow.a
  ${install_zlib}/lib/libz.a
  ${install_utf8}/lib/libutf8proc.a
  ${install_re2}/lib/libre2.a
  ${install_snappy}/lib/libsnappy.a
  ${install_zstd}/lib/libzstd.a
  # /opt/homebrew/lib/libthrift.a
  Threads::Threads)
