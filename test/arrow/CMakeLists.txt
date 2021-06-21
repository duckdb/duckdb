set(TEST_ARROW_OBJECTS abi_test.cpp parquet_test.cpp)

add_library_unity(test_arrow OBJECT ${TEST_ARROW_OBJECTS})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_arrow>
    PARENT_SCOPE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
include(ExternalProject)

ExternalProject_Add(
  arrow_ep
  GIT_REPOSITORY "https://github.com/apache/arrow"
  GIT_TAG f959141ece4d660bce5f7fa545befc0116a7db79
  PREFIX "${CMAKE_BINARY_DIR}/third_party/arrow"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow/install"
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Debug
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
    -DARROW_WITH_PROTOBUF=OFF -DARROW_WITH_RAPIDJSON=OFF -DARROW_WITH_SNAPPY=OFF
    -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=OFF -DBOOST_SOURCE=AUTO
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
  CONFIGURE_COMMAND
    ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/zlib/install
  UPDATE_COMMAND "")

ExternalProject_Get_Property(ZLIB install_dir)

set(install_zlib ${install_dir})

include_directories(${install_arrow}/include)
include_directories(../../third_party/catch)
