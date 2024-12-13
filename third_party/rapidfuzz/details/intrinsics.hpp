/* SPDX-License-Identifier: MIT */
/* Copyright © 2021 Max Bachmann */

#pragma once

#include <bitset>
#include <cassert>
#include <cstddef>
#include <limits>
#include <stdint.h>
#include <type_traits>

#if defined(_MSC_VER) && !defined(__clang__)
#    include <intrin.h>
#endif

namespace duckdb_rapidfuzz::detail {

template <typename T>
T bit_mask_lsb(size_t n)
{
    T mask = static_cast<T>(-1);
    if (n < sizeof(T) * 8) {
        mask += static_cast<T>(static_cast<T>(1) << n);
    }
    return mask;
}

template <typename T>
bool bittest(T a, int bit)
{
    return (a >> bit) & 1;
}

/*
 * shift right without undefined behavior for shifts > bit width
 */
template <typename U>
constexpr uint64_t shr64(uint64_t a, U shift)
{
    return (shift < 64) ? a >> shift : 0;
}

/*
 * shift left without undefined behavior for shifts > bit width
 */
template <typename U>
constexpr uint64_t shl64(uint64_t a, U shift)
{
    return (shift < 64) ? a << shift : 0;
}

constexpr uint64_t addc64(uint64_t a, uint64_t b, uint64_t carryin, uint64_t* carryout)
{
    /* todo should use _addcarry_u64 when available */
    a += carryin;
    *carryout = a < carryin;
    a += b;
    *carryout |= a < b;
    return a;
}

template <typename T, typename U>
constexpr T ceil_div(T a, U divisor)
{
    T _div = static_cast<T>(divisor);
    return a / _div + static_cast<T>(a % _div != 0);
}

static inline size_t popcount(uint64_t x)
{
    return std::bitset<64>(x).count();
}

static inline size_t popcount(uint32_t x)
{
    return std::bitset<32>(x).count();
}

static inline size_t popcount(uint16_t x)
{
    return std::bitset<16>(x).count();
}

static inline size_t popcount(uint8_t x)
{
    static constexpr uint8_t bit_count[256] = {
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
    return bit_count[x];
}

template <typename T>
constexpr T rotl(T x, unsigned int n)
{
    unsigned int num_bits = std::numeric_limits<T>::digits;
    assert(n < num_bits);
    unsigned int count_mask = num_bits - 1;

#if _MSC_VER && !defined(__clang__)
#    pragma warning(push)
/* unary minus operator applied to unsigned type, result still unsigned */
#    pragma warning(disable : 4146)
#endif
    return (x << n) | (x >> (-n & count_mask));
#if _MSC_VER && !defined(__clang__)
#    pragma warning(pop)
#endif
}

/**
 * Extract the lowest set bit from a. If no bits are set in a returns 0.
 */
template <typename T>
constexpr T blsi(T a)
{
#if _MSC_VER && !defined(__clang__)
#    pragma warning(push)
/* unary minus operator applied to unsigned type, result still unsigned */
#    pragma warning(disable : 4146)
#endif
    return a & -a;
#if _MSC_VER && !defined(__clang__)
#    pragma warning(pop)
#endif
}

/**
 * Clear the lowest set bit in a.
 */
template <typename T>
constexpr T blsr(T x)
{
    return x & (x - 1);
}

/**
 * Sets all the lower bits of the result to 1 up to and including lowest set bit (=1) in a.
 * If a is zero, blsmsk sets all bits to 1.
 */
template <typename T>
constexpr T blsmsk(T a)
{
    return a ^ (a - 1);
}

#if defined(_MSC_VER) && !defined(__clang__)
static inline unsigned int countr_zero(uint32_t x)
{
    unsigned long trailing_zero = 0;
    _BitScanForward(&trailing_zero, x);
    return trailing_zero;
}

#    if defined(_M_ARM) || defined(_M_X64)
static inline unsigned int countr_zero(uint64_t x)
{
    unsigned long trailing_zero = 0;
    _BitScanForward64(&trailing_zero, x);
    return trailing_zero;
}
#    else
static inline unsigned int countr_zero(uint64_t x)
{
    uint32_t msh = (uint32_t)(x >> 32);
    uint32_t lsh = (uint32_t)(x & 0xFFFFFFFF);
    if (lsh != 0) return countr_zero(lsh);
    return 32 + countr_zero(msh);
}
#    endif

#else /*  gcc / clang */
static inline unsigned int countr_zero(uint32_t x)
{
    return static_cast<unsigned int>(__builtin_ctz(x));
}

static inline unsigned int countr_zero(uint64_t x)
{
    return static_cast<unsigned int>(__builtin_ctzll(x));
}
#endif

static inline unsigned int countr_zero(uint16_t x)
{
    return countr_zero(static_cast<uint32_t>(x));
}

static inline unsigned int countr_zero(uint8_t x)
{
    return countr_zero(static_cast<uint32_t>(x));
}

template <class T, T... inds, class F>
constexpr void unroll_impl(std::integer_sequence<T, inds...>, F&& f)
{
    (f(std::integral_constant<T, inds>{}), ...);
}

template <class T, T count, class F>
constexpr void unroll(F&& f)
{
    unroll_impl(std::make_integer_sequence<T, count>{}, std::forward<F>(f));
}

} // namespace duckdb_rapidfuzz::detail
