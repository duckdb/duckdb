#if !defined(phmap_config_h_guard_)
#define phmap_config_h_guard_

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Includes work from abseil-cpp (https://github.com/abseil/abseil-cpp)
// with modifications.
//
// Copyright 2018 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ---------------------------------------------------------------------------

#define PHMAP_VERSION_MAJOR 2
#define PHMAP_VERSION_MINOR 0
#define PHMAP_VERSION_PATCH 0

// Included for the __GLIBC__ macro (or similar macros on other systems).
#include <limits.h>

#ifdef __cplusplus
    // Included for __GLIBCXX__, _LIBCPP_VERSION
    #include <cstddef>
#endif  // __cplusplus

#if defined(__APPLE__)
    // Included for TARGET_OS_IPHONE, __IPHONE_OS_VERSION_MIN_REQUIRED,
    // __IPHONE_8_0.
    #include <Availability.h>
    #include <TargetConditionals.h>
#endif

#define PHMAP_XSTR(x) PHMAP_STR(x)
#define PHMAP_STR(x) #x
#define PHMAP_VAR_NAME_VALUE(var) #var "="  PHMAP_STR(var)

// -----------------------------------------------------------------------------
// Some sanity checks
// -----------------------------------------------------------------------------
//#if defined(__CYGWIN__)
//    #error "Cygwin is not supported."
//#endif

#if defined(_MSC_FULL_VER) && _MSC_FULL_VER < 190023918 && !defined(__clang__)
    #error "phmap requires Visual Studio 2015 Update 2 or higher."
#endif

// We support gcc 4.7 and later.
#if defined(__GNUC__) && !defined(__clang__)
    #if __GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ < 7)
        #error "phmap requires gcc 4.7 or higher."
    #endif
#endif

// We support Apple Xcode clang 4.2.1 (version 421.11.65) and later.
// This corresponds to Apple Xcode version 4.5.
#if defined(__apple_build_version__) && __apple_build_version__ < 4211165
    #error "phmap requires __apple_build_version__ of 4211165 or higher."
#endif

// Enforce C++11 as the minimum.
#if defined(__cplusplus) && !defined(_MSC_VER)
    #if __cplusplus < 201103L
        #error "C++ versions less than C++11 are not supported."
    #endif
#endif

// We have chosen glibc 2.12 as the minimum
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
    #if !__GLIBC_PREREQ(2, 12)
        #error "Minimum required version of glibc is 2.12."
    #endif
#endif

#if defined(_STLPORT_VERSION)
    #error "STLPort is not supported."
#endif

#if CHAR_BIT != 8
    #warning "phmap assumes CHAR_BIT == 8."
#endif

// phmap currently assumes that an int is 4 bytes.
#if INT_MAX < 2147483647
    #error "phmap assumes that int is at least 4 bytes. "
#endif



// -----------------------------------------------------------------------------
// Compiler Feature Checks
// -----------------------------------------------------------------------------

#ifdef __has_builtin
    #define PHMAP_HAVE_BUILTIN(x) __has_builtin(x)
#else
    #define PHMAP_HAVE_BUILTIN(x) 0
#endif

#if (!defined(__GNUC__) || defined(__clang__) || __GNUC__ >= 5) && \
    ((defined(_MSVC_LANG) && _MSVC_LANG >= 201703L) || __cplusplus >= 201703L)
    #define PHMAP_HAVE_CC17 1
#else
    #define PHMAP_HAVE_CC17 0
#endif

#define PHMAP_BRANCHLESS 1

#ifdef __has_feature
#define PHMAP_HAVE_FEATURE(f) __has_feature(f)
#else
#define PHMAP_HAVE_FEATURE(f) 0
#endif

// Portable check for GCC minimum version:
// https://gcc.gnu.org/onlinedocs/cpp/Common-Predefined-Macros.html
#if defined(__GNUC__) && defined(__GNUC_MINOR__)
    #define PHMAP_INTERNAL_HAVE_MIN_GNUC_VERSION(x, y) (__GNUC__ > (x) || __GNUC__ == (x) && __GNUC_MINOR__ >= (y))
#else
    #define PHMAP_INTERNAL_HAVE_MIN_GNUC_VERSION(x, y) 0
#endif

#if defined(__clang__) && defined(__clang_major__) && defined(__clang_minor__)
    #define PHMAP_INTERNAL_HAVE_MIN_CLANG_VERSION(x, y) (__clang_major__ > (x) || __clang_major__ == (x) && __clang_minor__ >= (y))
#else
    #define PHMAP_INTERNAL_HAVE_MIN_CLANG_VERSION(x, y) 0
#endif

// -------------------------------------------------------------------
// Checks whether C++11's `thread_local` storage duration specifier is
// supported.
// -------------------------------------------------------------------
#ifdef PHMAP_HAVE_THREAD_LOCAL
    #error PHMAP_HAVE_THREAD_LOCAL cannot be directly set
#elif defined(__APPLE__) && defined(__clang__)
    #if __has_feature(cxx_thread_local) && \
        !(TARGET_OS_IPHONE && __IPHONE_OS_VERSION_MIN_REQUIRED < __IPHONE_9_0)
        #define PHMAP_HAVE_THREAD_LOCAL 1
    #endif
#else  // !defined(__APPLE__)
    #define PHMAP_HAVE_THREAD_LOCAL 1
#endif

#if defined(__ANDROID__) && defined(__clang__)

    #if __has_include(<android/ndk-version.h>)
        #include <android/ndk-version.h>
    #endif  // __has_include(<android/ndk-version.h>)

    #if defined(__ANDROID__) && defined(__clang__) && defined(__NDK_MAJOR__) && \
        defined(__NDK_MINOR__) &&                                               \
        ((__NDK_MAJOR__ < 12) || ((__NDK_MAJOR__ == 12) && (__NDK_MINOR__ < 1)))
        #undef PHMAP_HAVE_TLS
        #undef PHMAP_HAVE_THREAD_LOCAL
    #endif
#endif

// ------------------------------------------------------------
// Checks whether the __int128 compiler extension for a 128-bit
// integral type is supported.
// ------------------------------------------------------------
#ifdef PHMAP_HAVE_INTRINSIC_INT128
    #error PHMAP_HAVE_INTRINSIC_INT128 cannot be directly set
#elif defined(__SIZEOF_INT128__)
    #if (defined(__clang__) && !defined(_WIN32) && !defined(__aarch64__)) || \
        (defined(__CUDACC__) && __CUDACC_VER_MAJOR__ >= 9) ||                \
        (defined(__GNUC__) && !defined(__clang__) && !defined(__CUDACC__))
        #define PHMAP_HAVE_INTRINSIC_INT128 1
    #elif defined(__CUDACC__)
        #if __CUDACC_VER__ >= 70000
            #define PHMAP_HAVE_INTRINSIC_INT128 1
        #endif  // __CUDACC_VER__ >= 70000
    #endif  // defined(__CUDACC__)
#endif

// ------------------------------------------------------------------
// Checks whether the compiler both supports and enables exceptions.
// ------------------------------------------------------------------
#ifdef PHMAP_HAVE_EXCEPTIONS
    #error PHMAP_HAVE_EXCEPTIONS cannot be directly set.
#elif defined(__clang__)
    #if defined(__EXCEPTIONS) && __has_feature(cxx_exceptions)
        #define PHMAP_HAVE_EXCEPTIONS 1
    #endif  // defined(__EXCEPTIONS) && __has_feature(cxx_exceptions)
#elif !(defined(__GNUC__) && (__GNUC__ < 5) && !defined(__EXCEPTIONS)) &&    \
    !(defined(__GNUC__) && (__GNUC__ >= 5) && !defined(__cpp_exceptions)) && \
    !(defined(_MSC_VER) && !defined(_CPPUNWIND))
    #define PHMAP_HAVE_EXCEPTIONS 1
#endif


// -----------------------------------------------------------------------
// Checks whether the platform has an mmap(2) implementation as defined in
// POSIX.1-2001.
// -----------------------------------------------------------------------
#ifdef PHMAP_HAVE_MMAP
    #error PHMAP_HAVE_MMAP cannot be directly set
#elif defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) ||   \
    defined(__ros__) || defined(__native_client__) || defined(__asmjs__) || \
    defined(__wasm__) || defined(__Fuchsia__) || defined(__sun) || \
    defined(__ASYLO__)
    #define PHMAP_HAVE_MMAP 1
#endif

// -----------------------------------------------------------------------
// Checks the endianness of the platform.
// -----------------------------------------------------------------------
#if defined(PHMAP_IS_BIG_ENDIAN)
    #error "PHMAP_IS_BIG_ENDIAN cannot be directly set."
#endif

#if defined(PHMAP_IS_LITTLE_ENDIAN)
    #error "PHMAP_IS_LITTLE_ENDIAN cannot be directly set."
#endif

#if (defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
     __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    #define PHMAP_IS_LITTLE_ENDIAN 1
#elif defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && \
    __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    #define PHMAP_IS_BIG_ENDIAN 1
#elif defined(_WIN32)
    #define PHMAP_IS_LITTLE_ENDIAN 1
#else
    #error "phmap endian detection needs to be set up for your compiler"
#endif

#if defined(__APPLE__) && defined(_LIBCPP_VERSION) && \
    defined(__MAC_OS_X_VERSION_MIN_REQUIRED__) &&     \
    __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__ < 101400
    #define PHMAP_INTERNAL_MACOS_CXX17_TYPES_UNAVAILABLE 1
#else
    #define PHMAP_INTERNAL_MACOS_CXX17_TYPES_UNAVAILABLE 0
#endif

// ---------------------------------------------------------------------------
// Checks whether C++17 std::any is available by checking whether <any> exists.
// ---------------------------------------------------------------------------
#ifdef PHMAP_HAVE_STD_ANY
    #error "PHMAP_HAVE_STD_ANY cannot be directly set."
#endif

#ifdef __has_include
    #if __has_include(<any>) && __cplusplus >= 201703L && \
        !PHMAP_INTERNAL_MACOS_CXX17_TYPES_UNAVAILABLE
        #define PHMAP_HAVE_STD_ANY 1
    #endif
#endif

#ifdef PHMAP_HAVE_STD_OPTIONAL
    #error "PHMAP_HAVE_STD_OPTIONAL cannot be directly set."
#endif

#ifdef __has_include
    #if __has_include(<optional>) && __cplusplus >= 201703L && \
        !PHMAP_INTERNAL_MACOS_CXX17_TYPES_UNAVAILABLE
        #define PHMAP_HAVE_STD_OPTIONAL 1
    #endif
#endif

#ifdef PHMAP_HAVE_STD_VARIANT
    #error "PHMAP_HAVE_STD_VARIANT cannot be directly set."
#endif

#ifdef __has_include
    #if __has_include(<variant>) && __cplusplus >= 201703L && \
        !PHMAP_INTERNAL_MACOS_CXX17_TYPES_UNAVAILABLE
        #define PHMAP_HAVE_STD_VARIANT 1
    #endif
#endif

#ifdef PHMAP_HAVE_STD_STRING_VIEW
    #error "PHMAP_HAVE_STD_STRING_VIEW cannot be directly set."
#endif

#ifdef __has_include
    #if __has_include(<string_view>) && __cplusplus >= 201703L && \
        (!defined(_MSC_VER) || _MSC_VER >= 1920) // vs2019
        #define PHMAP_HAVE_STD_STRING_VIEW 1
    #endif
#endif

// #pragma message(PHMAP_VAR_NAME_VALUE(_MSVC_LANG))

#if defined(_MSC_VER) && _MSC_VER >= 1910 && PHMAP_HAVE_CC17
    // #define PHMAP_HAVE_STD_ANY 1
    #define PHMAP_HAVE_STD_OPTIONAL 1
    #define PHMAP_HAVE_STD_VARIANT 1
    #if !defined(PHMAP_HAVE_STD_STRING_VIEW) && _MSC_VER >= 1920
        #define PHMAP_HAVE_STD_STRING_VIEW 1
    #endif
#endif

#if PHMAP_HAVE_CC17
    #ifdef __has_include
       #if __has_include(<shared_mutex>)
           #define PHMAP_HAVE_SHARED_MUTEX 1
       #endif
    #endif
#endif

#ifndef PHMAP_HAVE_STD_STRING_VIEW
    #define PHMAP_HAVE_STD_STRING_VIEW 0
#endif

// In debug mode, MSVC 2017's std::variant throws a EXCEPTION_ACCESS_VIOLATION
// SEH exception from emplace for variant<SomeStruct> when constructing the
// struct can throw. This defeats some of variant_test and
// variant_exception_safety_test.
#if defined(_MSC_VER) && _MSC_VER >= 1700 && defined(_DEBUG)
    #define PHMAP_INTERNAL_MSVC_2017_DBG_MODE
#endif

// ---------------------------------------------------------------------------
// Checks whether wchar_t is treated as a native type
// (MSVC: /Zc:wchar_t- treats wchar_t as unsigned short)
// ---------------------------------------------------------------------------
#if !defined(_MSC_VER) || defined(_NATIVE_WCHAR_T_DEFINED)
#define PHMAP_HAS_NATIVE_WCHAR_T
#endif

// -----------------------------------------------------------------------------
// Sanitizer Attributes
// -----------------------------------------------------------------------------
//
// Sanitizer-related attributes are not "defined" in this file (and indeed
// are not defined as such in any file). To utilize the following
// sanitizer-related attributes within your builds, define the following macros
// within your build using a `-D` flag, along with the given value for
// `-fsanitize`:
//
//   * `ADDRESS_SANITIZER` + `-fsanitize=address` (Clang, GCC 4.8)
//   * `MEMORY_SANITIZER` + `-fsanitize=memory` (Clang-only)
//   * `THREAD_SANITIZER + `-fsanitize=thread` (Clang, GCC 4.8+)
//   * `UNDEFINED_BEHAVIOR_SANITIZER` + `-fsanitize=undefined` (Clang, GCC 4.9+)
//   * `CONTROL_FLOW_INTEGRITY` + -fsanitize=cfi (Clang-only)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// A function-like feature checking macro that is a wrapper around
// `__has_attribute`, which is defined by GCC 5+ and Clang and evaluates to a
// nonzero constant integer if the attribute is supported or 0 if not.
//
// It evaluates to zero if `__has_attribute` is not defined by the compiler.
// -----------------------------------------------------------------------------
#ifdef __has_attribute
    #define PHMAP_HAVE_ATTRIBUTE(x) __has_attribute(x)
#else
    #define PHMAP_HAVE_ATTRIBUTE(x) 0
#endif

// -----------------------------------------------------------------------------
// A function-like feature checking macro that accepts C++11 style attributes.
// It's a wrapper around `__has_cpp_attribute`, defined by ISO C++ SD-6
// (https://en.cppreference.com/w/cpp/experimental/feature_test). If we don't
// find `__has_cpp_attribute`, will evaluate to 0.
// -----------------------------------------------------------------------------
#if defined(__cplusplus) && defined(__has_cpp_attribute)
    #define PHMAP_HAVE_CPP_ATTRIBUTE(x) __has_cpp_attribute(x)
#else
    #define PHMAP_HAVE_CPP_ATTRIBUTE(x) 0
#endif

// -----------------------------------------------------------------------------
// Function Attributes
// -----------------------------------------------------------------------------
#if PHMAP_HAVE_ATTRIBUTE(format) || (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_PRINTF_ATTRIBUTE(string_index, first_to_check) \
      __attribute__((__format__(__printf__, string_index, first_to_check)))
    #define PHMAP_SCANF_ATTRIBUTE(string_index, first_to_check) \
      __attribute__((__format__(__scanf__, string_index, first_to_check)))
#else
    #define PHMAP_PRINTF_ATTRIBUTE(string_index, first_to_check)
    #define PHMAP_SCANF_ATTRIBUTE(string_index, first_to_check)
#endif

#if PHMAP_HAVE_ATTRIBUTE(always_inline) || \
    (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))
    #define PHMAP_HAVE_ATTRIBUTE_ALWAYS_INLINE 1
#else
    #define PHMAP_ATTRIBUTE_ALWAYS_INLINE
#endif

#if !defined(__INTEL_COMPILER) && (PHMAP_HAVE_ATTRIBUTE(noinline) || (defined(__GNUC__) && !defined(__clang__)))
    #define PHMAP_ATTRIBUTE_NOINLINE __attribute__((noinline))
    #define PHMAP_HAVE_ATTRIBUTE_NOINLINE 1
#else
    #define PHMAP_ATTRIBUTE_NOINLINE
#endif

#if PHMAP_HAVE_ATTRIBUTE(disable_tail_calls)
    #define PHMAP_HAVE_ATTRIBUTE_NO_TAIL_CALL 1
    #define PHMAP_ATTRIBUTE_NO_TAIL_CALL __attribute__((disable_tail_calls))
#elif defined(__GNUC__) && !defined(__clang__)
    #define PHMAP_HAVE_ATTRIBUTE_NO_TAIL_CALL 1
    #define PHMAP_ATTRIBUTE_NO_TAIL_CALL \
      __attribute__((optimize("no-optimize-sibling-calls")))
#else
    #define PHMAP_ATTRIBUTE_NO_TAIL_CALL
    #define PHMAP_HAVE_ATTRIBUTE_NO_TAIL_CALL 0
#endif

#if (PHMAP_HAVE_ATTRIBUTE(weak) || \
     (defined(__GNUC__) && !defined(__clang__))) && \
    !(defined(__llvm__) && defined(_WIN32))
    #undef PHMAP_ATTRIBUTE_WEAK
    #define PHMAP_ATTRIBUTE_WEAK __attribute__((weak))
    #define PHMAP_HAVE_ATTRIBUTE_WEAK 1
#else
    #define PHMAP_ATTRIBUTE_WEAK
    #define PHMAP_HAVE_ATTRIBUTE_WEAK 0
#endif

#if PHMAP_HAVE_ATTRIBUTE(nonnull) || (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_ATTRIBUTE_NONNULL(arg_index) __attribute__((nonnull(arg_index)))
#else
    #define PHMAP_ATTRIBUTE_NONNULL(...)
#endif

#if PHMAP_HAVE_ATTRIBUTE(noreturn)
    #define PHMAP_ATTRIBUTE_NORETURN [[noreturn]]
#elif defined(__GNUC__) && !defined(__clang__)
    #define PHMAP_ATTRIBUTE_NORETURN __attribute__((noreturn))
#elif defined(_MSC_VER)
    #define PHMAP_ATTRIBUTE_NORETURN __declspec(noreturn)
#else
    #define PHMAP_ATTRIBUTE_NORETURN
#endif

#if defined(__GNUC__) && defined(ADDRESS_SANITIZER)
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address))
#else
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_ADDRESS
#endif

#if defined(__GNUC__) && defined(MEMORY_SANITIZER)
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_MEMORY __attribute__((no_sanitize_memory))
#else
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_MEMORY
#endif

#if defined(__GNUC__) && defined(THREAD_SANITIZER)
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_THREAD __attribute__((no_sanitize_thread))
#else
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_THREAD
#endif

#if defined(__GNUC__) && \
    (defined(UNDEFINED_BEHAVIOR_SANITIZER) || defined(ADDRESS_SANITIZER))
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_UNDEFINED \
      __attribute__((no_sanitize("undefined")))
#else
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_UNDEFINED
#endif

#if defined(__GNUC__) && defined(CONTROL_FLOW_INTEGRITY)
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_CFI __attribute__((no_sanitize("cfi")))
#else
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_CFI
#endif

#if defined(__GNUC__) && defined(SAFESTACK_SANITIZER)
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_SAFESTACK \
      __attribute__((no_sanitize("safe-stack")))
#else
    #define PHMAP_ATTRIBUTE_NO_SANITIZE_SAFESTACK
#endif

#if PHMAP_HAVE_ATTRIBUTE(returns_nonnull) || \
    (defined(__GNUC__) && \
     (__GNUC__ > 5 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 9)) && \
     !defined(__clang__))
    #define PHMAP_ATTRIBUTE_RETURNS_NONNULL __attribute__((returns_nonnull))
#else
    #define PHMAP_ATTRIBUTE_RETURNS_NONNULL
#endif

#ifdef PHMAP_HAVE_ATTRIBUTE_SECTION
    #error PHMAP_HAVE_ATTRIBUTE_SECTION cannot be directly set
#elif (PHMAP_HAVE_ATTRIBUTE(section) ||                \
       (defined(__GNUC__) && !defined(__clang__))) && \
    !defined(__APPLE__) && PHMAP_HAVE_ATTRIBUTE_WEAK
    #define PHMAP_HAVE_ATTRIBUTE_SECTION 1
    #ifndef PHMAP_ATTRIBUTE_SECTION
        #define PHMAP_ATTRIBUTE_SECTION(name) \
          __attribute__((section(#name))) __attribute__((noinline))
    #endif
    #ifndef PHMAP_ATTRIBUTE_SECTION_VARIABLE
        #define PHMAP_ATTRIBUTE_SECTION_VARIABLE(name) __attribute__((section(#name)))
    #endif
    #ifndef PHMAP_DECLARE_ATTRIBUTE_SECTION_VARS
        #define PHMAP_DECLARE_ATTRIBUTE_SECTION_VARS(name) \
          extern char __start_##name[] PHMAP_ATTRIBUTE_WEAK;    \
          extern char __stop_##name[] PHMAP_ATTRIBUTE_WEAK
    #endif
    #ifndef PHMAP_DEFINE_ATTRIBUTE_SECTION_VARS
        #define PHMAP_INIT_ATTRIBUTE_SECTION_VARS(name)
        #define PHMAP_DEFINE_ATTRIBUTE_SECTION_VARS(name)
    #endif
    #define PHMAP_ATTRIBUTE_SECTION_START(name) \
      (reinterpret_cast<void *>(__start_##name))
    #define PHMAP_ATTRIBUTE_SECTION_STOP(name) \
      (reinterpret_cast<void *>(__stop_##name))
#else  // !PHMAP_HAVE_ATTRIBUTE_SECTION
    #define PHMAP_HAVE_ATTRIBUTE_SECTION 0
    #define PHMAP_ATTRIBUTE_SECTION(name)
    #define PHMAP_ATTRIBUTE_SECTION_VARIABLE(name)
    #define PHMAP_INIT_ATTRIBUTE_SECTION_VARS(name)
    #define PHMAP_DEFINE_ATTRIBUTE_SECTION_VARS(name)
    #define PHMAP_DECLARE_ATTRIBUTE_SECTION_VARS(name)
    #define PHMAP_ATTRIBUTE_SECTION_START(name) (reinterpret_cast<void *>(0))
    #define PHMAP_ATTRIBUTE_SECTION_STOP(name) (reinterpret_cast<void *>(0))
#endif  // PHMAP_ATTRIBUTE_SECTION

#if PHMAP_HAVE_ATTRIBUTE(force_align_arg_pointer) || \
    (defined(__GNUC__) && !defined(__clang__))
    #if defined(__i386__)
        #define PHMAP_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC \
          __attribute__((force_align_arg_pointer))
        #define PHMAP_REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
    #elif defined(__x86_64__)
        #define PHMAP_REQUIRE_STACK_ALIGN_TRAMPOLINE (1)
        #define PHMAP_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
    #else  // !__i386__ && !__x86_64
        #define PHMAP_REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
        #define PHMAP_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
    #endif  // __i386__
#else
    #define PHMAP_ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
    #define PHMAP_REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#endif

#if PHMAP_HAVE_ATTRIBUTE(nodiscard)
    #define PHMAP_MUST_USE_RESULT [[nodiscard]]
#elif defined(__clang__) && PHMAP_HAVE_ATTRIBUTE(warn_unused_result)
    #define PHMAP_MUST_USE_RESULT __attribute__((warn_unused_result))
#else
    #define PHMAP_MUST_USE_RESULT
#endif

#if PHMAP_HAVE_ATTRIBUTE(hot) || (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_ATTRIBUTE_HOT __attribute__((hot))
#else
    #define PHMAP_ATTRIBUTE_HOT
#endif

#if PHMAP_HAVE_ATTRIBUTE(cold) || (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_ATTRIBUTE_COLD __attribute__((cold))
#else
    #define PHMAP_ATTRIBUTE_COLD
#endif

#if defined(__clang__)
    #if PHMAP_HAVE_CPP_ATTRIBUTE(clang::reinitializes)
        #define PHMAP_ATTRIBUTE_REINITIALIZES [[clang::reinitializes]]
    #else
        #define PHMAP_ATTRIBUTE_REINITIALIZES
    #endif
#else
    #define PHMAP_ATTRIBUTE_REINITIALIZES
#endif

#if PHMAP_HAVE_ATTRIBUTE(unused) || (defined(__GNUC__) && !defined(__clang__))
    #undef PHMAP_ATTRIBUTE_UNUSED
    #define PHMAP_ATTRIBUTE_UNUSED __attribute__((__unused__))
#else
    #define PHMAP_ATTRIBUTE_UNUSED
#endif

#if PHMAP_HAVE_ATTRIBUTE(tls_model) || (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_ATTRIBUTE_INITIAL_EXEC __attribute__((tls_model("initial-exec")))
#else
    #define PHMAP_ATTRIBUTE_INITIAL_EXEC
#endif

#if PHMAP_HAVE_ATTRIBUTE(packed) || (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_ATTRIBUTE_PACKED __attribute__((__packed__))
#else
    #define PHMAP_ATTRIBUTE_PACKED
#endif

#if PHMAP_HAVE_ATTRIBUTE(aligned) || (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_ATTRIBUTE_FUNC_ALIGN(bytes) __attribute__((aligned(bytes)))
#else
    #define PHMAP_ATTRIBUTE_FUNC_ALIGN(bytes)
#endif

// ----------------------------------------------------------------------
// Figure out SSE support
// ----------------------------------------------------------------------
#ifndef PHMAP_HAVE_SSE2
    #if defined(__SSE2__) ||  \
        (defined(_MSC_VER) && \
         (defined(_M_X64) || (defined(_M_IX86) && _M_IX86_FP >= 2)))
        #define PHMAP_HAVE_SSE2 1
    #else
        #define PHMAP_HAVE_SSE2 0
    #endif
#endif

#ifndef PHMAP_HAVE_SSSE3
    #if defined(__SSSE3__) || defined(__AVX2__)
        #define PHMAP_HAVE_SSSE3 1
    #else
        #define PHMAP_HAVE_SSSE3 0
    #endif
#endif

#if PHMAP_HAVE_SSSE3 && !PHMAP_HAVE_SSE2
    #error "Bad configuration!"
#endif

#if PHMAP_HAVE_SSE2
    #include <emmintrin.h>
#endif

#if PHMAP_HAVE_SSSE3
    #include <tmmintrin.h>
#endif


// ----------------------------------------------------------------------
// constexpr if
// ----------------------------------------------------------------------
#if PHMAP_HAVE_CC17
    #define PHMAP_IF_CONSTEXPR(expr) if constexpr ((expr))
#else
    #define PHMAP_IF_CONSTEXPR(expr) if ((expr))
#endif

// ----------------------------------------------------------------------
// builtin unreachable
// ----------------------------------------------------------------------
#if PHMAP_HAVE_BUILTIN(__builtin_unreachable)
    #define PHMAP_BUILTIN_UNREACHABLE() __builtin_unreachable()
#else
    #define PHMAP_BUILTIN_UNREACHABLE() (void)0
#endif

// ----------------------------------------------------------------------
// RESTRICT
// ----------------------------------------------------------------------
#if (defined(__GNUC__) && (__GNUC__ > 3)) || defined(__clang__)
    #define PHMAP_RESTRICT __restrict__
#elif defined(_MSC_VER) && _MSC_VER >= 1400
    #define PHMAP_RESTRICT __restrict
#else
    #define PHMAP_RESTRICT
#endif

// ----------------------------------------------------------------------
// base/macros.h
// ----------------------------------------------------------------------

// PHMAP_ARRAYSIZE()
//
// Returns the number of elements in an array as a compile-time constant, which
// can be used in defining new arrays. If you use this macro on a pointer by
// mistake, you will get a compile-time error.
#define PHMAP_ARRAYSIZE(array) \
  (sizeof(::phmap::macros_internal::ArraySizeHelper(array)))

namespace phmap {
namespace macros_internal {
    // Note: this internal template function declaration is used by PHMAP_ARRAYSIZE.
    // The function doesn't need a definition, as we only use its type.
    template <typename T, size_t N>
    auto ArraySizeHelper(const T (&array)[N]) -> char (&)[N];
}  // namespace macros_internal
}  // namespace phmap

#if PHMAP_HAVE_CPP_ATTRIBUTE(fallthrough)
    #define PHMAP_FALLTHROUGH [[fallthrough]]
#elif defined(__clang__) && defined(__has_warning)
    #if __has_feature(cxx_attributes) && __has_warning("-Wimplicit-fallthrough")
        #define PHMAP_FALLTHROUGH_INTENDED [[clang::fallthrough]]
    #endif
#elif defined(__GNUC__) && __GNUC__ >= 7
    #define PHMAP_FALLTHROUGH_INTENDED [[gnu::fallthrough]]
#endif

#ifndef PHMAP_FALLTHROUGH_INTENDED
    #define PHMAP_FALLTHROUGH_INTENDED \
      do {  } while (0)
#endif

// PHMAP_DEPRECATED()
//
// Marks a deprecated class, struct, enum, function, method and variable
// declarations. The macro argument is used as a custom diagnostic message (e.g.
// suggestion of a better alternative).
//
// Example:
//
//   class PHMAP_DEPRECATED("Use Bar instead") Foo {...};
//   PHMAP_DEPRECATED("Use Baz instead") void Bar() {...}
//
// Every usage of a deprecated entity will trigger a warning when compiled with
// clang's `-Wdeprecated-declarations` option. This option is turned off by
// default, but the warnings will be reported by clang-tidy.
#if defined(__clang__) && __cplusplus >= 201103L
    #define PHMAP_DEPRECATED(message) __attribute__((deprecated(message)))
#endif

#ifndef PHMAP_DEPRECATED
    #define PHMAP_DEPRECATED(message)
#endif

// PHMAP_BAD_CALL_IF()
//
// Used on a function overload to trap bad calls: any call that matches the
// overload will cause a compile-time error. This macro uses a clang-specific
// "enable_if" attribute, as described at
// http://clang.llvm.org/docs/AttributeReference.html#enable-if
//
// Overloads which use this macro should be bracketed by
// `#ifdef PHMAP_BAD_CALL_IF`.
//
// Example:
//
//   int isdigit(int c);
//   #ifdef PHMAP_BAD_CALL_IF
//   int isdigit(int c)
//     PHMAP_BAD_CALL_IF(c <= -1 || c > 255,
//                       "'c' must have the value of an unsigned char or EOF");
//   #endif // PHMAP_BAD_CALL_IF

#if defined(__clang__)
    #if __has_attribute(enable_if)
        #define PHMAP_BAD_CALL_IF(expr, msg) \
            __attribute__((enable_if(expr, "Bad call trap"), unavailable(msg)))
    #endif
#endif

// PHMAP_ASSERT()
//
// In C++11, `assert` can't be used portably within constexpr functions.
// PHMAP_ASSERT functions as a runtime assert but works in C++11 constexpr
// functions.  Example:
//
// constexpr double Divide(double a, double b) {
//   return PHMAP_ASSERT(b != 0), a / b;
// }
//
// This macro is inspired by
// https://akrzemi1.wordpress.com/2017/05/18/asserts-in-constexpr-functions/
#if defined(NDEBUG)
    #define PHMAP_ASSERT(expr) (false ? (void)(expr) : (void)0)
#else
    #define PHMAP_ASSERT(expr)              \
      (PHMAP_PREDICT_TRUE((expr)) ? (void)0 \
                                 : [] { assert(false && #expr); }())  // NOLINT
#endif

#ifdef PHMAP_HAVE_EXCEPTIONS
    #define PHMAP_INTERNAL_TRY try
    #define PHMAP_INTERNAL_CATCH_ANY catch (...)
    #define PHMAP_INTERNAL_RETHROW do { throw; } while (false)
#else  // PHMAP_HAVE_EXCEPTIONS
    #define PHMAP_INTERNAL_TRY if (true)
    #define PHMAP_INTERNAL_CATCH_ANY else if (false)
    #define PHMAP_INTERNAL_RETHROW do {} while (false)
#endif  // PHMAP_HAVE_EXCEPTIONS


#endif // phmap_config_h_guard_
