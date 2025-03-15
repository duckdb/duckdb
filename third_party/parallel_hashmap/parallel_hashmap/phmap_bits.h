#if !defined(phmap_bits_h_guard_)
#define phmap_bits_h_guard_

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

// The following guarantees declaration of the byte swap functions
#ifdef _MSC_VER
    #include <stdlib.h>  // NOLINT(build/include)
#elif defined(__APPLE__)
    // Mac OS X / Darwin features
    #include <libkern/OSByteOrder.h>
#elif defined(__FreeBSD__)
    #include <sys/endian.h>
#elif defined(__GLIBC__)
    #include <byteswap.h>  // IWYU pragma: export
#endif

#include <string.h>
#include <cstdint>
#include "phmap_config.h"

#ifdef _MSC_VER
    #pragma warning(push)  
    #pragma warning(disable : 4514) // unreferenced inline function has been removed
#endif

// -----------------------------------------------------------------------------
// unaligned APIs
// -----------------------------------------------------------------------------
// Portable handling of unaligned loads, stores, and copies.
// On some platforms, like ARM, the copy functions can be more efficient
// then a load and a store.
// -----------------------------------------------------------------------------

#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) ||\
    defined(MEMORY_SANITIZER)
#include <stdint.h>

extern "C" {
    uint16_t __sanitizer_unaligned_load16(const void *p);
    uint32_t __sanitizer_unaligned_load32(const void *p);
    uint64_t __sanitizer_unaligned_load64(const void *p);
    void __sanitizer_unaligned_store16(void *p, uint16_t v);
    void __sanitizer_unaligned_store32(void *p, uint32_t v);
    void __sanitizer_unaligned_store64(void *p, uint64_t v);
}  // extern "C"

namespace phmap {
namespace bits {

inline uint16_t UnalignedLoad16(const void *p) {
  return __sanitizer_unaligned_load16(p);
}

inline uint32_t UnalignedLoad32(const void *p) {
  return __sanitizer_unaligned_load32(p);
}

inline uint64_t UnalignedLoad64(const void *p) {
  return __sanitizer_unaligned_load64(p);
}

inline void UnalignedStore16(void *p, uint16_t v) {
  __sanitizer_unaligned_store16(p, v);
}

inline void UnalignedStore32(void *p, uint32_t v) {
  __sanitizer_unaligned_store32(p, v);
}

inline void UnalignedStore64(void *p, uint64_t v) {
  __sanitizer_unaligned_store64(p, v);
}

}  // namespace bits
}  // namespace phmap

#define PHMAP_INTERNAL_UNALIGNED_LOAD16(_p) (phmap::bits::UnalignedLoad16(_p))
#define PHMAP_INTERNAL_UNALIGNED_LOAD32(_p) (phmap::bits::UnalignedLoad32(_p))
#define PHMAP_INTERNAL_UNALIGNED_LOAD64(_p) (phmap::bits::UnalignedLoad64(_p))

#define PHMAP_INTERNAL_UNALIGNED_STORE16(_p, _val) (phmap::bits::UnalignedStore16(_p, _val))
#define PHMAP_INTERNAL_UNALIGNED_STORE32(_p, _val) (phmap::bits::UnalignedStore32(_p, _val))
#define PHMAP_INTERNAL_UNALIGNED_STORE64(_p, _val) (phmap::bits::UnalignedStore64(_p, _val))

#else

namespace phmap {
namespace bits {

inline uint16_t UnalignedLoad16(const void *p) {
  uint16_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint32_t UnalignedLoad32(const void *p) {
  uint32_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint64_t UnalignedLoad64(const void *p) {
  uint64_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UnalignedStore16(void *p, uint16_t v) { memcpy(p, &v, sizeof v); }

inline void UnalignedStore32(void *p, uint32_t v) { memcpy(p, &v, sizeof v); }

inline void UnalignedStore64(void *p, uint64_t v) { memcpy(p, &v, sizeof v); }

}  // namespace bits
}  // namespace phmap

#define PHMAP_INTERNAL_UNALIGNED_LOAD16(_p) (phmap::bits::UnalignedLoad16(_p))
#define PHMAP_INTERNAL_UNALIGNED_LOAD32(_p) (phmap::bits::UnalignedLoad32(_p))
#define PHMAP_INTERNAL_UNALIGNED_LOAD64(_p) (phmap::bits::UnalignedLoad64(_p))

#define PHMAP_INTERNAL_UNALIGNED_STORE16(_p, _val) (phmap::bits::UnalignedStore16(_p, _val))
#define PHMAP_INTERNAL_UNALIGNED_STORE32(_p, _val) (phmap::bits::UnalignedStore32(_p, _val))
#define PHMAP_INTERNAL_UNALIGNED_STORE64(_p, _val) (phmap::bits::UnalignedStore64(_p, _val))

#endif

// -----------------------------------------------------------------------------
// File: optimization.h
// -----------------------------------------------------------------------------

#if defined(__pnacl__)
    #define PHMAP_BLOCK_TAIL_CALL_OPTIMIZATION() if (volatile int x = 0) { (void)x; }
#elif defined(__clang__)
    // Clang will not tail call given inline volatile assembly.
    #define PHMAP_BLOCK_TAIL_CALL_OPTIMIZATION() __asm__ __volatile__("")
#elif defined(__GNUC__)
    // GCC will not tail call given inline volatile assembly.
    #define PHMAP_BLOCK_TAIL_CALL_OPTIMIZATION() __asm__ __volatile__("")
#elif defined(_MSC_VER)
    #include <intrin.h>
    // The __nop() intrinsic blocks the optimisation.
    #define PHMAP_BLOCK_TAIL_CALL_OPTIMIZATION() __nop()
#else
    #define PHMAP_BLOCK_TAIL_CALL_OPTIMIZATION() if (volatile int x = 0) { (void)x; }
#endif

#if defined(__GNUC__)
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wpedantic"
#endif

#ifdef PHMAP_HAVE_INTRINSIC_INT128
    __extension__ typedef unsigned __int128 phmap_uint128;
    inline uint64_t umul128(uint64_t a, uint64_t b, uint64_t* high) 
    {
        auto result = static_cast<phmap_uint128>(a) * static_cast<phmap_uint128>(b);
        *high = static_cast<uint64_t>(result >> 64);
        return static_cast<uint64_t>(result);
    }
    #define PHMAP_HAS_UMUL128 1
#elif (defined(_MSC_VER))
    #if defined(_M_X64)
        #pragma intrinsic(_umul128)
        inline uint64_t umul128(uint64_t a, uint64_t b, uint64_t* high) 
        {
            return _umul128(a, b, high);
        }
        #define PHMAP_HAS_UMUL128 1
    #endif
#endif

#if defined(__GNUC__)
    #pragma GCC diagnostic pop
#endif

#if defined(__GNUC__)
    // Cache line alignment
    #if defined(__i386__) || defined(__x86_64__)
        #define PHMAP_CACHELINE_SIZE 64
    #elif defined(__powerpc64__)
        #define PHMAP_CACHELINE_SIZE 128
    #elif defined(__aarch64__)
        // We would need to read special register ctr_el0 to find out L1 dcache size.
        // This value is a good estimate based on a real aarch64 machine.
        #define PHMAP_CACHELINE_SIZE 64
    #elif defined(__arm__)
        // Cache line sizes for ARM: These values are not strictly correct since
        // cache line sizes depend on implementations, not architectures.  There
        // are even implementations with cache line sizes configurable at boot
        // time.
        #if defined(__ARM_ARCH_5T__)
            #define PHMAP_CACHELINE_SIZE 32
        #elif defined(__ARM_ARCH_7A__)
            #define PHMAP_CACHELINE_SIZE 64
        #endif
    #endif

    #ifndef PHMAP_CACHELINE_SIZE
        // A reasonable default guess.  Note that overestimates tend to waste more
        // space, while underestimates tend to waste more time.
        #define PHMAP_CACHELINE_SIZE 64
    #endif

    #define PHMAP_CACHELINE_ALIGNED __attribute__((aligned(PHMAP_CACHELINE_SIZE)))
#elif defined(_MSC_VER)
    #define PHMAP_CACHELINE_SIZE 64
    #define PHMAP_CACHELINE_ALIGNED __declspec(align(PHMAP_CACHELINE_SIZE))
#else
    #define PHMAP_CACHELINE_SIZE 64
    #define PHMAP_CACHELINE_ALIGNED
#endif


#if PHMAP_HAVE_BUILTIN(__builtin_expect) || \
    (defined(__GNUC__) && !defined(__clang__))
    #define PHMAP_PREDICT_FALSE(x) (__builtin_expect(x, 0))
    #define PHMAP_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
    #define PHMAP_PREDICT_FALSE(x) (x)
    #define PHMAP_PREDICT_TRUE(x) (x)
#endif

// -----------------------------------------------------------------------------
// File: bits.h
// -----------------------------------------------------------------------------

#if defined(_MSC_VER)
    // We can achieve something similar to attribute((always_inline)) with MSVC by
    // using the __forceinline keyword, however this is not perfect. MSVC is
    // much less aggressive about inlining, and even with the __forceinline keyword.
    #define PHMAP_BASE_INTERNAL_FORCEINLINE __forceinline
#else
    // Use default attribute inline.
    #define PHMAP_BASE_INTERNAL_FORCEINLINE inline PHMAP_ATTRIBUTE_ALWAYS_INLINE
#endif


namespace phmap {
namespace base_internal {

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountLeadingZeros64Slow(uint64_t n) {
    int zeroes = 60;
    if (n >> 32) zeroes -= 32, n >>= 32;
    if (n >> 16) zeroes -= 16, n >>= 16;
    if (n >> 8) zeroes -= 8, n >>= 8;
    if (n >> 4) zeroes -= 4, n >>= 4;
    return (uint32_t)("\4\3\2\2\1\1\1\1\0\0\0\0\0\0\0"[n] + zeroes);
}

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountLeadingZeros64(uint64_t n) {
#if defined(_MSC_VER) && defined(_M_X64)
    // MSVC does not have __buitin_clzll. Use _BitScanReverse64.
    unsigned long result = 0;  // NOLINT(runtime/int)
    if (_BitScanReverse64(&result, n)) {
        return (uint32_t)(63 - result);
    }
    return 64;
#elif defined(_MSC_VER) && !defined(__clang__)
    // MSVC does not have __buitin_clzll. Compose two calls to _BitScanReverse
    unsigned long result = 0;  // NOLINT(runtime/int)
    if ((n >> 32) && _BitScanReverse(&result, (unsigned long)(n >> 32))) {
        return  (uint32_t)(31 - result);
    }
    if (_BitScanReverse(&result, (unsigned long)n)) {
        return (uint32_t)(63 - result);
    }
    return 64;
#elif defined(__GNUC__) || defined(__clang__)
    // Use __builtin_clzll, which uses the following instructions:
    //  x86: bsr
    //  ARM64: clz
    //  PPC: cntlzd
    static_assert(sizeof(unsigned long long) == sizeof(n),  // NOLINT(runtime/int)
                  "__builtin_clzll does not take 64-bit arg");

    // Handle 0 as a special case because __builtin_clzll(0) is undefined.
    if (n == 0) {
        return 64;
    }
    return  (uint32_t)__builtin_clzll(n);
#else
    return CountLeadingZeros64Slow(n);
#endif
}

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountLeadingZeros32Slow(uint64_t n) {
    uint32_t zeroes = 28;
    if (n >> 16) zeroes -= 16, n >>= 16;
    if (n >> 8) zeroes -= 8, n >>= 8;
    if (n >> 4) zeroes -= 4, n >>= 4;
    return "\4\3\2\2\1\1\1\1\0\0\0\0\0\0\0"[n] + zeroes;
}

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountLeadingZeros32(uint32_t n) {
#if defined(_MSC_VER) && !defined(__clang__)
    unsigned long result = 0;  // NOLINT(runtime/int)
    if (_BitScanReverse(&result, n)) {
        return (uint32_t)(31 - result);
    }
    return 32;
#elif defined(__GNUC__) || defined(__clang__)
    // Use __builtin_clz, which uses the following instructions:
    //  x86: bsr
    //  ARM64: clz
    //  PPC: cntlzd
    static_assert(sizeof(int) == sizeof(n),
                  "__builtin_clz does not take 32-bit arg");

    // Handle 0 as a special case because __builtin_clz(0) is undefined.
    if (n == 0) {
        return 32;
    }
    return __builtin_clz(n);
#else
    return CountLeadingZeros32Slow(n);
#endif
}

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountTrailingZerosNonZero64Slow(uint64_t n) {
    uint32_t c = 63;
    n &= ~n + 1;
    if (n & 0x00000000FFFFFFFF) c -= 32;
    if (n & 0x0000FFFF0000FFFF) c -= 16;
    if (n & 0x00FF00FF00FF00FF) c -= 8;
    if (n & 0x0F0F0F0F0F0F0F0F) c -= 4;
    if (n & 0x3333333333333333) c -= 2;
    if (n & 0x5555555555555555) c -= 1;
    return c;
}

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountTrailingZerosNonZero64(uint64_t n) {
#if defined(_MSC_VER) && !defined(__clang__) && defined(_M_X64)
    unsigned long result = 0;  // NOLINT(runtime/int)
    _BitScanForward64(&result, n);
    return (uint32_t)result;
#elif defined(_MSC_VER) && !defined(__clang__)
    unsigned long result = 0;  // NOLINT(runtime/int)
    if (static_cast<uint32_t>(n) == 0) {
        _BitScanForward(&result, (unsigned long)(n >> 32));
        return result + 32;
    }
    _BitScanForward(&result, (unsigned long)n);
    return result;
#elif defined(__GNUC__) || defined(__clang__)
    static_assert(sizeof(unsigned long long) == sizeof(n),  // NOLINT(runtime/int)
                  "__builtin_ctzll does not take 64-bit arg");
    return __builtin_ctzll(n);
#else
    return CountTrailingZerosNonZero64Slow(n);
#endif
}

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountTrailingZerosNonZero32Slow(uint32_t n) {
    uint32_t c = 31;
    n &= ~n + 1;
    if (n & 0x0000FFFF) c -= 16;
    if (n & 0x00FF00FF) c -= 8;
    if (n & 0x0F0F0F0F) c -= 4;
    if (n & 0x33333333) c -= 2;
    if (n & 0x55555555) c -= 1;
    return c;
}

PHMAP_BASE_INTERNAL_FORCEINLINE uint32_t CountTrailingZerosNonZero32(uint32_t n) {
#if defined(_MSC_VER) && !defined(__clang__)
    unsigned long result = 0;  // NOLINT(runtime/int)
    _BitScanForward(&result, n);
    return (uint32_t)result;
#elif defined(__GNUC__) || defined(__clang__)
    static_assert(sizeof(int) == sizeof(n),
                  "__builtin_ctz does not take 32-bit arg");
    return __builtin_ctz(n);
#else
    return CountTrailingZerosNonZero32Slow(n);
#endif
}

#undef PHMAP_BASE_INTERNAL_FORCEINLINE

}  // namespace base_internal
}  // namespace phmap

// -----------------------------------------------------------------------------
// File: endian.h
// -----------------------------------------------------------------------------

namespace phmap {

// Use compiler byte-swapping intrinsics if they are available.  32-bit
// and 64-bit versions are available in Clang and GCC as of GCC 4.3.0.
// The 16-bit version is available in Clang and GCC only as of GCC 4.8.0.
// For simplicity, we enable them all only for GCC 4.8.0 or later.
#if defined(__clang__) || \
    (defined(__GNUC__) && \
     ((__GNUC__ == 4 && __GNUC_MINOR__ >= 8) || __GNUC__ >= 5))

    inline uint64_t gbswap_64(uint64_t host_int) {
        return __builtin_bswap64(host_int);
    }
    inline uint32_t gbswap_32(uint32_t host_int) {
        return __builtin_bswap32(host_int);
    }
    inline uint16_t gbswap_16(uint16_t host_int) {
        return __builtin_bswap16(host_int);
    }

#elif defined(_MSC_VER)

    inline uint64_t gbswap_64(uint64_t host_int) {
        return _byteswap_uint64(host_int);
    }
    inline uint32_t gbswap_32(uint32_t host_int) {
        return _byteswap_ulong(host_int);
    }
    inline uint16_t gbswap_16(uint16_t host_int) {
        return _byteswap_ushort(host_int);
    }

#elif defined(__APPLE__)

    inline uint64_t gbswap_64(uint64_t host_int) { return OSSwapInt16(host_int); }
    inline uint32_t gbswap_32(uint32_t host_int) { return OSSwapInt32(host_int); }
    inline uint16_t gbswap_16(uint16_t host_int) { return OSSwapInt64(host_int); }

#else

    inline uint64_t gbswap_64(uint64_t host_int) {
#if defined(__GNUC__) && defined(__x86_64__) && !defined(__APPLE__)
        // Adapted from /usr/include/byteswap.h.  Not available on Mac.
        if (__builtin_constant_p(host_int)) {
            return __bswap_constant_64(host_int);
        } else {
            uint64_t result;
            __asm__("bswap %0" : "=r"(result) : "0"(host_int));
            return result;
        }
#elif defined(__GLIBC__)
        return bswap_64(host_int);
#else
        return (((host_int & uint64_t{0xFF}) << 56) |
                ((host_int & uint64_t{0xFF00}) << 40) |
                ((host_int & uint64_t{0xFF0000}) << 24) |
                ((host_int & uint64_t{0xFF000000}) << 8) |
                ((host_int & uint64_t{0xFF00000000}) >> 8) |
                ((host_int & uint64_t{0xFF0000000000}) >> 24) |
                ((host_int & uint64_t{0xFF000000000000}) >> 40) |
                ((host_int & uint64_t{0xFF00000000000000}) >> 56));
#endif  // bswap_64
    }

    inline uint32_t gbswap_32(uint32_t host_int) {
#if defined(__GLIBC__)
        return bswap_32(host_int);
#else
        return (((host_int & uint32_t{0xFF}) << 24) |
                ((host_int & uint32_t{0xFF00}) << 8) |
                ((host_int & uint32_t{0xFF0000}) >> 8) |
                ((host_int & uint32_t{0xFF000000}) >> 24));
#endif
    }

    inline uint16_t gbswap_16(uint16_t host_int) {
#if defined(__GLIBC__)
        return bswap_16(host_int);
#else
        return (((host_int & uint16_t{0xFF}) << 8) |
                ((host_int & uint16_t{0xFF00}) >> 8));
#endif
    }

#endif  // intrinics available

#ifdef PHMAP_IS_LITTLE_ENDIAN

    // Definitions for ntohl etc. that don't require us to include
    // netinet/in.h. We wrap gbswap_32 and gbswap_16 in functions rather
    // than just #defining them because in debug mode, gcc doesn't
    // correctly handle the (rather involved) definitions of bswap_32.
    // gcc guarantees that inline functions are as fast as macros, so
    // this isn't a performance hit.
    inline uint16_t ghtons(uint16_t x) { return gbswap_16(x); }
    inline uint32_t ghtonl(uint32_t x) { return gbswap_32(x); }
    inline uint64_t ghtonll(uint64_t x) { return gbswap_64(x); }

#elif defined PHMAP_IS_BIG_ENDIAN

    // These definitions are simpler on big-endian machines
    // These are functions instead of macros to avoid self-assignment warnings
    // on calls such as "i = ghtnol(i);".  This also provides type checking.
    inline uint16_t ghtons(uint16_t x) { return x; }
    inline uint32_t ghtonl(uint32_t x) { return x; }
    inline uint64_t ghtonll(uint64_t x) { return x; }

#else
    #error \
        "Unsupported byte order: Either PHMAP_IS_BIG_ENDIAN or " \
           "PHMAP_IS_LITTLE_ENDIAN must be defined"
#endif  // byte order

inline uint16_t gntohs(uint16_t x) { return ghtons(x); }
inline uint32_t gntohl(uint32_t x) { return ghtonl(x); }
inline uint64_t gntohll(uint64_t x) { return ghtonll(x); }

// Utilities to convert numbers between the current hosts's native byte
// order and little-endian byte order
//
// Load/Store methods are alignment safe
namespace little_endian {
// Conversion functions.
#ifdef PHMAP_IS_LITTLE_ENDIAN

    inline uint16_t FromHost16(uint16_t x) { return x; }
    inline uint16_t ToHost16(uint16_t x) { return x; }

    inline uint32_t FromHost32(uint32_t x) { return x; }
    inline uint32_t ToHost32(uint32_t x) { return x; }

    inline uint64_t FromHost64(uint64_t x) { return x; }
    inline uint64_t ToHost64(uint64_t x) { return x; }

    inline constexpr bool IsLittleEndian() { return true; }

#elif defined PHMAP_IS_BIG_ENDIAN

    inline uint16_t FromHost16(uint16_t x) { return gbswap_16(x); }
    inline uint16_t ToHost16(uint16_t x) { return gbswap_16(x); }

    inline uint32_t FromHost32(uint32_t x) { return gbswap_32(x); }
    inline uint32_t ToHost32(uint32_t x) { return gbswap_32(x); }

    inline uint64_t FromHost64(uint64_t x) { return gbswap_64(x); }
    inline uint64_t ToHost64(uint64_t x) { return gbswap_64(x); }

    inline constexpr bool IsLittleEndian() { return false; }

#endif /* ENDIAN */

// Functions to do unaligned loads and stores in little-endian order.
// ------------------------------------------------------------------
inline uint16_t Load16(const void *p) {
  return ToHost16(PHMAP_INTERNAL_UNALIGNED_LOAD16(p));
}

inline void Store16(void *p, uint16_t v) {
  PHMAP_INTERNAL_UNALIGNED_STORE16(p, FromHost16(v));
}

inline uint32_t Load32(const void *p) {
  return ToHost32(PHMAP_INTERNAL_UNALIGNED_LOAD32(p));
}

inline void Store32(void *p, uint32_t v) {
  PHMAP_INTERNAL_UNALIGNED_STORE32(p, FromHost32(v));
}

inline uint64_t Load64(const void *p) {
  return ToHost64(PHMAP_INTERNAL_UNALIGNED_LOAD64(p));
}

inline void Store64(void *p, uint64_t v) {
  PHMAP_INTERNAL_UNALIGNED_STORE64(p, FromHost64(v));
}

}  // namespace little_endian

// Utilities to convert numbers between the current hosts's native byte
// order and big-endian byte order (same as network byte order)
//
// Load/Store methods are alignment safe
namespace big_endian {
#ifdef PHMAP_IS_LITTLE_ENDIAN

    inline uint16_t FromHost16(uint16_t x) { return gbswap_16(x); }
    inline uint16_t ToHost16(uint16_t x) { return gbswap_16(x); }

    inline uint32_t FromHost32(uint32_t x) { return gbswap_32(x); }
    inline uint32_t ToHost32(uint32_t x) { return gbswap_32(x); }

    inline uint64_t FromHost64(uint64_t x) { return gbswap_64(x); }
    inline uint64_t ToHost64(uint64_t x) { return gbswap_64(x); }

    inline constexpr bool IsLittleEndian() { return true; }

#elif defined PHMAP_IS_BIG_ENDIAN

    inline uint16_t FromHost16(uint16_t x) { return x; }
    inline uint16_t ToHost16(uint16_t x) { return x; }

    inline uint32_t FromHost32(uint32_t x) { return x; }
    inline uint32_t ToHost32(uint32_t x) { return x; }

    inline uint64_t FromHost64(uint64_t x) { return x; }
    inline uint64_t ToHost64(uint64_t x) { return x; }

    inline constexpr bool IsLittleEndian() { return false; }

#endif /* ENDIAN */

// Functions to do unaligned loads and stores in big-endian order.
inline uint16_t Load16(const void *p) {
  return ToHost16(PHMAP_INTERNAL_UNALIGNED_LOAD16(p));
}

inline void Store16(void *p, uint16_t v) {
  PHMAP_INTERNAL_UNALIGNED_STORE16(p, FromHost16(v));
}

inline uint32_t Load32(const void *p) {
  return ToHost32(PHMAP_INTERNAL_UNALIGNED_LOAD32(p));
}

inline void Store32(void *p, uint32_t v) {
  PHMAP_INTERNAL_UNALIGNED_STORE32(p, FromHost32(v));
}

inline uint64_t Load64(const void *p) {
  return ToHost64(PHMAP_INTERNAL_UNALIGNED_LOAD64(p));
}

inline void Store64(void *p, uint64_t v) {
  PHMAP_INTERNAL_UNALIGNED_STORE64(p, FromHost64(v));
}

}  // namespace big_endian

}  // namespace phmap

#ifdef _MSC_VER
     #pragma warning(pop)  
#endif

#endif // phmap_bits_h_guard_
