cmake_minimum_required(VERSION 2.8.12)

project(SIMDExtension)

# Detect cpu architecture
set(detect_arch "
#if defined(__arm__)
    #error arm
#elif defined(__x86_64) || defined(__x86_64__) || defined(__amd64) || defined(_M_X64)
    #error x86_64
#endif
#error fallback
")

set(ARCH "FALLBACK")
file(WRITE "${CMAKE_BINARY_DIR}/arch.cpp" "${detect_arch}")
try_run(run_result
        compile_result
        "${CMAKE_BINARY_DIR}"
        "${CMAKE_BINARY_DIR}/arch.cpp"
        COMPILE_OUTPUT_VARIABLE ARCH
        )

string(REGEX MATCH "error: ([a-zA-Z0-9_]+)" ARCH "${ARCH}")
string(REPLACE "error: " "" ARCH "${ARCH}")

if(ARCH STREQUAL "x86_64")
    add_compile_definitions(DUCKDB_X86_64)
elseif(ARCH STREQUAL "arm")
    add_compile_definitions(DUCKDB_ARM)
else()
    add_compile_definitions(DUCKDB_FALLBACK)
endif()

include_directories(include)

add_library(simd_extension STATIC simd-extension.cpp)