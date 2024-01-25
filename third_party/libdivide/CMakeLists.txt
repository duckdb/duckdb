cmake_minimum_required(VERSION 3.5)
set(LIBDIVIDE_VERSION "5.0")
project(libdivide C CXX)

include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)
include(CheckCXXSourceCompiles)
include(CheckCXXSourceRuns)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CMakePushCheckState)
include(CMakeSanitize)

# Compile options ################################################

# Maximum warnings level & warnings as error
add_compile_options(
    "$<$<CXX_COMPILER_ID:MSVC>:/W4;/WX>"
    "$<$<CXX_COMPILER_ID:GNU>:-Wall;-Wextra;-pedantic;-Werror>"
    "$<$<CXX_COMPILER_ID:Clang>:-Wall;-Wextra;-pedantic;-Werror>"
    "$<$<CXX_COMPILER_ID:AppleClang>:-Wall;-Wextra;-pedantic;-Werror>"
)

# Build options ################################################

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(MAIN_PROJECT ON)
else()
  set(MAIN_PROJECT OFF)
endif()

option(LIBDIVIDE_BUILD_TESTS   "Build the test programs" "${MAIN_PROJECT}")
option(LIBDIVIDE_BUILD_FUZZERS "Build the fuzzers (requires clang)" OFF)

# By default we automatically enable vectors supported by
# the host CPU. You may also set these explicitly to OFF or ON.
set(LIBDIVIDE_SSE2  AUTO CACHE STRING  "Enable SSE2 vector instructions")
set(LIBDIVIDE_AVX2  AUTO CACHE STRING  "Enable AVX2 vector instructions")
set(LIBDIVIDE_AVX512 AUTO CACHE STRING "Enable AVX512 vector instructions")
set(LIBDIVIDE_NEON AUTO CACHE STRING "Enable ARM NEON vector instructions")

# By default enable release mode ###############################

if(NOT CMAKE_VERSION VERSION_LESS 3.9)
    get_property(isMultiConfig GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
elseif(CMAKE_CONFIGURATION_TYPES)
    set(isMultiConfig TRUE)
endif()

if(NOT isMultiConfig AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING
    "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif()

# Enable assertions in debug mode ##############################

string(TOUPPER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)

if("${BUILD_TYPE}" MATCHES DEBUG)
    set(LIBDIVIDE_ASSERTIONS -DLIBDIVIDE_ASSERTIONS_ON)
endif()

# Try to warn about bogus vector conversions ###################
set(LIBDIVIDE_FLAGS)
check_cxx_compiler_flag(-fno-lax-vector-conversions WARN_VEC_CONVERSIONS)
if (WARN_VEC_CONVERSIONS)
    list(APPEND LIBDIVIDE_FLAGS "-fno-lax-vector-conversions")
endif()


# Check if x86/x64 CPU  ########################################

# Note that check_cxx_source_runs() must not be used when
# cross-compiling otherwise the following error will occur:
# CMake Error: TRY_RUN() invoked in cross-compiling mode, ...

if (LIBDIVIDE_BUILD_TESTS AND NOT CMAKE_CROSSCOMPILING)
    check_cxx_source_compiles("
        int main()
        {
            #if !defined(__i386__) && \
                !defined(__x86_64__) && \
                !defined(_M_IX86) && \
                !defined(_M_X64)
                #error not x86 CPU architecture
            #endif
            return 0;
        }"
        CPU_X86)
    check_cxx_source_compiles("
        #include <arm_neon.h>
        int main()
        {
            #if !defined(__ARM_NEON)
                #error not ARM NEON CPU architecture
            #endif
            return 0;
        }"
        CPU_ARM_NEON)
    if (CPU_X86 OR CPU_ARM_NEON)
        cmake_push_check_state()
        set(CMAKE_REQUIRED_FLAGS -Werror)
        check_cxx_compiler_flag(-march=native MARCH_NATIVE)
        cmake_pop_check_state()

        if (MARCH_NATIVE)
            list(APPEND LIBDIVIDE_FLAGS "-march=native")
        endif()
    endif()
endif()

# Disable auto vectorization ###################################

# We disable auto vectorization on x86 (and x64-64) in order
# to prevent the compiler from vectorizing our scalar benchmarks
# which would make the benchmark results less useful.

if(CPU_X86 OR CPU_ARM_NEON)
    # first check the C++ compiler
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        check_cxx_compiler_flag("-fno-vectorize" fno_vectorize)
        if(fno_vectorize)
            set(NO_VECTORIZE -fno-vectorize)
        endif()
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        check_cxx_compiler_flag("-fno-tree-vectorize" fno_tree_vectorize)
        if(fno_tree_vectorize)
            set(NO_VECTORIZE -fno-tree-vectorize)
        endif()
    endif()
    # then check the C compiler
    if("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
        check_c_compiler_flag("-fno-vectorize" fno_vectorize_c)
        if(fno_vectorize_c)
            set(NO_VECTORIZE_C -fno-vectorize)
        endif()
    elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
        check_c_compiler_flag("-fno-tree-vectorize" fno_tree_vectorize_c)
        if(fno_tree_vectorize_c)
            set(NO_VECTORIZE_C -fno-tree-vectorize)
        endif()
    endif()
endif()

# Resolve any "auto" supported vector types ##################
# Note these use "check_cxx_source_runs" and not "check_cxx_source_compiles"
# because VC++ will compile AVX512 even though it's not available at runtime.
cmake_push_check_state()
# Need to convert from list to space-separated string.
string(REPLACE ";" " " CMAKE_REQUIRED_FLAGS "${LIBDIVIDE_FLAGS}")

# NEON
if (NOT LIBDIVIDE_NEON STREQUAL "AUTO")
    set(LIBDIVIDE_NEON_ENABLED "${LIBDIVIDE_NEON}")
else()
    set(NEON_TEST "
    #include <arm_neon.h>
    int main()
    {
        int32x4_t a = vdupq_n_s32(0);
        return *(int *)&a;
    }")
    if (CMAKE_CROSSCOMPILING)
        check_cxx_source_compiles("${NEON_TEST}" LIBDIVIDE_NEON_ENABLED)
    else()
        check_cxx_source_runs("${NEON_TEST}" LIBDIVIDE_NEON_ENABLED)
    endif()
endif()

# AVX512
if (NOT LIBDIVIDE_AVX512 STREQUAL "AUTO")
    set(LIBDIVIDE_AVX512_ENABLED "${LIBDIVIDE_AVX512}")
else()
    check_cxx_source_runs("
                #include <immintrin.h>
                int main()
                {
                    __m512i a = _mm512_setzero_si512();
                    return *(int *)&a;
                }"
                LIBDIVIDE_AVX512_ENABLED)
endif()

# AVX2
if (NOT LIBDIVIDE_AVX2 STREQUAL "AUTO")
    set(LIBDIVIDE_AVX2_ENABLED "${LIBDIVIDE_AVX2}")
else()
    # Need to use an AVX2 insn like srai, not just AVX.
    check_cxx_source_runs("
                #include <immintrin.h>
                int main()
                {
                    __m256i a = _mm256_srai_epi32(_mm256_setzero_si256(), 3);
                    return *(int *)&a;
                }"
                LIBDIVIDE_AVX2_ENABLED)
endif()

# SSE2
if (NOT LIBDIVIDE_SSE2 STREQUAL "AUTO")
    set(LIBDIVIDE_SSE2_ENABLED "${LIBDIVIDE_SSE2}")
else()
    check_cxx_source_runs("
                #include <emmintrin.h>
                int main()
                {
                    __m128i a = _mm_setzero_si128();
                    return *(int *)&a;
                }"
                LIBDIVIDE_SSE2_ENABLED)
endif()
cmake_pop_check_state()


# Populate LIBDIVIDE_VECTOR_EXT ##################
set(LIBDIVIDE_VECTOR_EXT "")
if(LIBDIVIDE_NEON_ENABLED)
    list(APPEND LIBDIVIDE_VECTOR_EXT "LIBDIVIDE_NEON")
endif()
if(LIBDIVIDE_AVX512_ENABLED)
    list(APPEND LIBDIVIDE_VECTOR_EXT "LIBDIVIDE_AVX512")
endif()
if(LIBDIVIDE_AVX2_ENABLED)
    list(APPEND LIBDIVIDE_VECTOR_EXT "LIBDIVIDE_AVX2")
endif()
if(LIBDIVIDE_SSE2_ENABLED)
    list(APPEND LIBDIVIDE_VECTOR_EXT "LIBDIVIDE_SSE2")
endif()

# libdivide header-only library target #########################

add_library(libdivide INTERFACE)
add_library(libdivide::libdivide ALIAS libdivide)

target_compile_features(libdivide INTERFACE cxx_alias_templates)

target_include_directories(libdivide INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>)

install(FILES libdivide.h
        COMPONENT libdivide-header
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

# CMake find_package(libdivide) support ########################

install(TARGETS libdivide
        EXPORT libdivideConfig
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")

export(TARGETS libdivide
       NAMESPACE libdivide::
       FILE "${CMAKE_CURRENT_BINARY_DIR}/libdivideConfig.cmake")

install(EXPORT libdivideConfig
        NAMESPACE libdivide::
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/libdivide")

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/libdivideConfigVersion.cmake"
    VERSION ${LIBDIVIDE_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/libdivideConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/libdivide")

# Build test programs ##########################################

if (LIBDIVIDE_BUILD_TESTS)
    find_package(Threads REQUIRED QUIET)

    add_executable(tester test/tester.cpp)
    add_executable(test_c99 test/test_c99.c)
    add_executable(fast_div_generator test/fast_div_generator.cpp)
    add_executable(benchmark test/benchmark.cpp)
    add_executable(benchmark_branchfree test/benchmark_branchfree.cpp)

    target_link_libraries(tester libdivide Threads::Threads)
    target_link_libraries(test_c99 libdivide)
    target_link_libraries(fast_div_generator libdivide)
    target_link_libraries(benchmark libdivide)
    target_link_libraries(benchmark_branchfree libdivide)

    target_compile_options(tester PRIVATE "${LIBDIVIDE_FLAGS}" "${NO_VECTORIZE}")
    target_compile_options(test_c99 PRIVATE "${LIBDIVIDE_FLAGS}" "${NO_VECTORIZE}")
    target_compile_options(fast_div_generator PRIVATE "${LIBDIVIDE_FLAGS}" "${NO_VECTORIZE}")
    target_compile_options(benchmark PRIVATE "${LIBDIVIDE_FLAGS}" "${NO_VECTORIZE_C}")
    target_compile_options(benchmark_branchfree PRIVATE "${LIBDIVIDE_FLAGS}" "${NO_VECTORIZE}")
    set_property(TARGET benchmark_branchfree PROPERTY CXX_STANDARD 11)
    set_property(TARGET test_c99 PROPERTY C_STANDARD 99)

    target_compile_definitions(tester PRIVATE "${LIBDIVIDE_ASSERTIONS}" "${LIBDIVIDE_VECTOR_EXT}")
    target_compile_definitions(test_c99 PRIVATE "${LIBDIVIDE_ASSERTIONS}" "${LIBDIVIDE_VECTOR_EXT}")
    target_compile_definitions(fast_div_generator PRIVATE "${LIBDIVIDE_ASSERTIONS}" "${LIBDIVIDE_VECTOR_EXT}")
    target_compile_definitions(benchmark PRIVATE "${LIBDIVIDE_ASSERTIONS}" "${LIBDIVIDE_VECTOR_EXT}")
    target_compile_definitions(benchmark_branchfree PRIVATE "${LIBDIVIDE_ASSERTIONS}" "${LIBDIVIDE_VECTOR_EXT}")
endif()

# Enable testing ###############################################

if (LIBDIVIDE_BUILD_TESTS)
    include(CTest)
    enable_testing()
    add_test(tester tester)
    add_test(benchmark_branchfree benchmark_branchfree)

    # cmake won't actually build the tests before it tries to run them
    add_test(build_tester "${CMAKE_COMMAND}" --build ${CMAKE_BINARY_DIR} --target tester)
    add_test(build_benchmark_branchfree "${CMAKE_COMMAND}" --build ${CMAKE_BINARY_DIR} --target benchmark_branchfree)
    set_tests_properties(tester benchmark_branchfree PROPERTIES DEPENDS "build_tester;build_benchmark_branchfree")
endif()

# Build the fuzzers (requires clang) ###########################

if (LIBDIVIDE_BUILD_FUZZERS)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(fuzzflags "-fsanitize=fuzzer,address")

        add_executable(fuzzer_scalar test/fuzzer_scalar.cpp)
        target_compile_features(fuzzer_scalar PRIVATE cxx_std_17)
        target_link_libraries(fuzzer_scalar PRIVATE "${fuzzflags}")
        target_compile_options(fuzzer_scalar PRIVATE "${NATIVE_FLAG}" "${NO_VECTORIZE}" "${fuzzflags}")
        target_compile_definitions(fuzzer_scalar PRIVATE "${LIBDIVIDE_ASSERTIONS}")
        target_link_libraries(fuzzer_scalar PRIVATE libdivide)

        if (LIBDIVIDE_VECTOR_EXT)
            add_executable(fuzzer_simd test/fuzzer_simd.cpp)
            target_compile_features(fuzzer_simd PRIVATE cxx_std_17)
            target_link_libraries(fuzzer_simd PRIVATE "${fuzzflags}")
            target_compile_options(fuzzer_simd PRIVATE "${NATIVE_FLAG}" "${NO_VECTORIZE}" "${fuzzflags}")
            target_compile_definitions(fuzzer_simd PRIVATE "${LIBDIVIDE_ASSERTIONS}" "${LIBDIVIDE_VECTOR_EXT}")
            target_link_libraries(fuzzer_simd PRIVATE libdivide)
        endif()
    else()
        message(FATAL_ERROR "You must use clang to build the fuzzers (uses libFuzzer)")
    endif()
endif()
