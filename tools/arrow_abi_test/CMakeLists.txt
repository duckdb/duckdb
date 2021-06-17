# Always build the abi test with asan to reveal problems with the release
# callbacks
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
include(ExternalProject)


ExternalProject_Add(
  arrow_ep
  GIT_REPOSITORY "https://github.com/apache/arrow"
  GIT_TAG f959141ece4d660bce5f7fa545befc0116a7db79
  PREFIX "${CMAKE_BINARY_DIR}/third_party/arrow"
  INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow/install"
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
    -DARROW_WITH_PROTOBUF=OFF -DARROW_WITH_RAPIDJSON=OFF -DARROW_WITH_SNAPPY=OFF
    -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=OFF -DBOOST_SOURCE=AUTO
    <SOURCE_DIR>/cpp
  UPDATE_COMMAND "")

ExternalProject_Get_Property(arrow_ep install_dir)



include_directories(${install_dir}/include)

add_executable(ba ${CMAKE_CURRENT_SOURCE_DIR}/abi_test.cc)

include_directories(ba
                    ${CMAKE_CURRENT_SOURCE_DIR}/../../src/include)

include_directories(../../third_party/catch)

include ( "re2.cmake" )
set(RE2_STATIC_LIBRARIES -L${RE2_LIBRARY_DIR} -l libre2.a )

include ( "utf8.cmake" )
set(UTF8_STATIC_LIBRARIES -L${UTF8_LIBRARY_DIR} -l libutf8proc.a )

include ( "zlib.cmake" )
set(ZLIB_STATIC_LIBRARIES -L${ZLIB_LIBRARY_DIR} -l libz.a )

add_dependencies(ba RE2 arrow_ep   UTF8 ZLIB duckdb)
message (${install_dir})
target_link_libraries(ba PRIVATE  duckdb ${install_dir}/lib/libparquet.a ${install_dir}/lib/libarrow_bundled_dependencies.a  ${install_dir}/lib/libarrow.a   ${ZLIB_STATIC_LIBRARIES} ${UTF8_STATIC_LIBRARIES} /usr/local/lib/libre2.a Threads::Threads)

#target_link_libraries(ba parquet)