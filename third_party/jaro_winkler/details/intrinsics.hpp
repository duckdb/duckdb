/* SPDX-License-Identifier: MIT */
/* Copyright © 2022 Max Bachmann */

#pragma once

#include <cstdint>

#if defined(_MSC_VER) && !defined(__clang__)
#    include <intrin.h>
#endif

namespace duckdb_jaro_winkler {
namespace intrinsics {

template <typename T>
T bit_mask_lsb(int n)
{
    T mask = -1;
    if (n < static_cast<int>(sizeof(T) * 8)) {
        mask += static_cast<T>(1) << n;
    }
    return mask;
}

template <typename T>
bool bittest(T a, int bit)
{
    return (a >> bit) & 1;
}

static inline int64_t popcount(uint64_t x)
{
    const uint64_t m1 = 0x5555555555555555;
    const uint64_t m2 = 0x3333333333333333;
    const uint64_t m4 = 0x0f0f0f0f0f0f0f0f;
    const uint64_t h01 = 0x0101010101010101;

    x -= (x >> 1) & m1;
    x = (x & m2) + ((x >> 2) & m2);
    x = (x + (x >> 4)) & m4;
    return static_cast<int64_t>((x * h01) >> 56);
}

/**
 * Extract the lowest set bit from a. If no bits are set in a returns 0.
 */
template <typename T>
T blsi(T a)
{
#if _MSC_VER && !defined(__clang__)
#  pragma warning(push)
/* unary minus operator applied to unsigned type, result still unsigned */
#  pragma warning(disable: 4146)
#endif
    return a & -a;
#if _MSC_VER && !defined(__clang__)
#  pragma warning(pop)
#endif
}

/**
 * Clear the lowest set bit in a.
 */
template <typename T>
T blsr(T x)
{
    return x & (x - 1);
}

#if defined(_MSC_VER) && !defined(__clang__)
static inline int tzcnt(uint32_t x)
{
    unsigned long trailing_zero = 0;
    _BitScanForward(&trailing_zero, x);
    return trailing_zero;
}

#    if defined(_M_ARM) || defined(_M_X64)
static inline int tzcnt(uint64_t x)
{
    unsigned long trailing_zero = 0;
    _BitScanForward64(&trailing_zero, x);
    return trailing_zero;
}
#    else
static inline int tzcnt(uint64_t x)
{
    uint32_t msh = (uint32_t)(x >> 32);
    uint32_t lsh = (uint32_t)(x & 0xFFFFFFFF);
    if (lsh != 0) {
        return tzcnt(lsh);
    }
    return 32 + tzcnt(msh);
}
#    endif

#else /*  gcc / clang */
//static inline int tzcnt(uint32_t x)
//{
//    return __builtin_ctz(x);
//}

static inline int tzcnt(uint64_t x)
{
    return __builtin_ctzll(x);
}
#endif

} // namespace intrinsics
} // namespace duckdb_jaro_winkler
