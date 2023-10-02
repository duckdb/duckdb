# AVX512 instrinsics are disabled by default, uncomment this to enable
#add_definitions(-DDUCKDB_FSST_ENABLE_INTRINSINCS)

if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

find_package(Threads REQUIRED)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)

# Note: disabled march=native for now
# TODO add march=native flag to duckdb
#include(CheckCXXCompilerFlag)
#check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
#if(COMPILER_SUPPORTS_MARCH_NATIVE)
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
#endif()

# Disabled due to issues on Windows builds
#if(CMAKE_BUILD_TYPE STREQUAL Release)
#    set_source_files_properties(
#            fsst_avx512.cpp
#            PROPERTIES
#            COMPILE_FLAGS -O1
#    )
#endif()

add_library(duckdb_fsst STATIC libfsst.cpp fsst_avx512.cpp fsst_avx512_unroll1.inc fsst_avx512_unroll2.inc fsst_avx512_unroll3.inc fsst_avx512_unroll4.inc)

target_include_directories(duckdb_fsst PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(duckdb_fsst PROPERTIES EXPORT_NAME duckdb_fsst)

install(TARGETS duckdb_fsst
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_fsst)