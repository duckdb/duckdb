#ifndef FASTMOD_H
#define FASTMOD_H

#ifndef __cplusplus
#include <stdbool.h>
#include <stdint.h>
#else
// In C++ <cstdbool>/<stdbool.h> are irelevant as bool is already a type
#include <cstdint>
#endif

#ifndef __cplusplus
#define FASTMOD_API static inline
#else
// In C++ we mark all the functions inline.
// If C++14 relaxed constexpr is supported we use constexpr so functions
// can be used at compile-time.
#if __cpp_constexpr >= 201304 && !defined(_MSC_VER)
// visual studio does not like constexpr
#define FASTMOD_API constexpr
#define FASTMOD_CONSTEXPR constexpr
#else
#define FASTMOD_API inline
#define FASTMOD_CONSTEXPR
#endif
#endif

#ifdef _MSC_VER
#include <intrin.h>
#endif

#ifdef __cplusplus
namespace fastmod {
#endif

#ifdef _MSC_VER

// __umulh is only available in x64 mode under Visual Studio: don't compile to 32-bit!
FASTMOD_API uint64_t mul128_u32(uint64_t lowbits, uint32_t d) {
  return __umulh(lowbits, d);
}

#else // _MSC_VER NOT defined

FASTMOD_API uint64_t mul128_u32(uint64_t lowbits, uint32_t d) {
  return ((__uint128_t)lowbits * d) >> 64;
}

FASTMOD_API uint64_t mul128_s32(uint64_t lowbits, int32_t d) {
  return ((__int128_t)lowbits * d) >> 64;
}

// This is for the 64-bit functions.
FASTMOD_API uint64_t mul128_u64(__uint128_t lowbits, uint64_t d) {
  __uint128_t bottom_half = (lowbits & UINT64_C(0xFFFFFFFFFFFFFFFF)) * d; // Won't overflow
  bottom_half >>= 64;  // Only need the top 64 bits, as we'll shift the lower half away;
  __uint128_t top_half = (lowbits >> 64) * d;
  __uint128_t both_halves = bottom_half + top_half; // Both halves are already shifted down by 64
  both_halves >>= 64; // Get top half of both_halves
  return (uint64_t)both_halves;
}

#endif // _MSC_VER

/**
 * Unsigned integers.
 * Usage:
 *  uint32_t d = ... ; // divisor, should be non-zero
 *  uint64_t M = computeM_u32(d); // do once
 *  fastmod_u32(a,M,d) is a % d for all 32-bit a.
 *
 **/


// M = ceil( (1<<64) / d ), d > 0
FASTMOD_API uint64_t computeM_u32(uint32_t d) {
  return UINT64_C(0xFFFFFFFFFFFFFFFF) / d + 1;
}

// fastmod computes (a % d) given precomputed M
FASTMOD_API uint32_t fastmod_u32(uint32_t a, uint64_t M, uint32_t d) {
  uint64_t lowbits = M * a;
  return (uint32_t)(mul128_u32(lowbits, d));
}

// fastdiv computes (a / d) given precomputed M for d>1
FASTMOD_API uint32_t fastdiv_u32(uint32_t a, uint64_t M) {
  return (uint32_t)(mul128_u32(M, a));
}

// given precomputed M, is_divisible checks whether n % d == 0
FASTMOD_API bool is_divisible(uint32_t n, uint64_t M) {
  return n * M <= M - 1;
}

/**
 * signed integers
 * Usage:
 *  int32_t d = ... ; // should be non-zero and between [-2147483647,2147483647]
 *  int32_t positive_d = d < 0 ? -d : d; // absolute value
 *  uint64_t M = computeM_s32(d); // do once
 *  fastmod_s32(a,M,positive_d) is a % d for all 32-bit a.
 **/

// M = floor( (1<<64) / d ) + 1
// you must have that d is different from 0 and -2147483648
// if d = -1 and a = -2147483648, the result is undefined
FASTMOD_API uint64_t computeM_s32(int32_t d) {
  if (d < 0)
    d = -d;
  return UINT64_C(0xFFFFFFFFFFFFFFFF) / d + 1 + ((d & (d - 1)) == 0 ? 1 : 0);
}

// fastmod computes (a % d) given precomputed M,
// you should pass the absolute value of d
FASTMOD_API int32_t fastmod_s32(int32_t a, uint64_t M, int32_t positive_d) {
  uint64_t lowbits = M * a;
  int32_t highbits = (int32_t)mul128_u32(lowbits, positive_d);
  return highbits - ((positive_d - 1) & (a >> 31));
}

#ifndef _MSC_VER
// fastdiv computes (a / d) given a precomputed M, assumes that d must not
// be one of -1, 1, or -2147483648
FASTMOD_API int32_t fastdiv_s32(int32_t a, uint64_t M, int32_t d) {
  uint64_t highbits = mul128_s32(M, a);
  highbits += (a < 0 ? 1 : 0);
  if (d < 0)
    return -(int32_t)(highbits);
  return (int32_t)(highbits);
}

// What follows is the 64-bit functions.
// They are currently not supported on Visual Studio
// due to the lack of a mul128_u64 function.
// They may not be faster than what the compiler
// can produce.

FASTMOD_API __uint128_t computeM_u64(uint64_t d) {
  // what follows is just ((__uint128_t)0 - 1) / d) + 1 spelled out
  __uint128_t M = UINT64_C(0xFFFFFFFFFFFFFFFF);
  M <<= 64;
  M |= UINT64_C(0xFFFFFFFFFFFFFFFF);
  M /= d;
  M += 1;
  return M;
}

FASTMOD_API __uint128_t computeM_s64(int64_t d) {
  if (d < 0)
    d = -d;
  __uint128_t M = UINT64_C(0xFFFFFFFFFFFFFFFF);
  M <<= 64;
  M |= UINT64_C(0xFFFFFFFFFFFFFFFF);
  M /= d;
  M += 1;
  M += ((d & (d - 1)) == 0 ? 1 : 0);
  return M;
}

FASTMOD_API uint64_t fastmod_u64(uint64_t a, __uint128_t M, uint64_t d) {
  __uint128_t lowbits = M * a;
  return mul128_u64(lowbits, d);
}

FASTMOD_API uint64_t fastdiv_u64(uint64_t a, __uint128_t M) {
  return mul128_u64(M, a);
}

// End of the 64-bit functions

#endif // #ifndef _MSC_VER

#ifdef __cplusplus

template<uint32_t d>
FASTMOD_API uint32_t fastmod(uint32_t x) {
    FASTMOD_CONSTEXPR uint64_t v = computeM_u32(d);
    return fastmod_u32(x, v, d);
}
template<uint32_t d>
FASTMOD_API uint32_t fastdiv(uint32_t x) {
    FASTMOD_CONSTEXPR uint64_t v = computeM_u32(d);
    return fastdiv_u32(x, v);
}
template<int32_t d>
FASTMOD_API int32_t fastmod(int32_t x) {
    FASTMOD_CONSTEXPR uint64_t v = computeM_s32(d);
    return fastmod_s32(x, v, d);
}
template<int32_t d>
FASTMOD_API int32_t fastdiv(int32_t x) {
    FASTMOD_CONSTEXPR uint64_t v = computeM_s32(d);
    return fastdiv_s32(x, v, d);
}

} // fastmod
#endif


// There's no reason to polute the global scope with this macro once its use ends
// This won't create any problems as the preprocessor will have done its thing once
// it reaches this point
#undef FASTMOD_API
#undef FASTMOD_CONSTEXPR

#endif // FASTMOD_H
