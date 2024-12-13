/* SPDX-License-Identifier: MIT */
/* Copyright © 2022 Max Bachmann */
#pragma once

#include <array>
#include <emmintrin.h>
#include <ostream>
#include <rapidfuzz/details/intrinsics.hpp>
#include <stdint.h>

namespace duckdb_rapidfuzz {
namespace detail {
namespace simd_sse2 {

template <typename T>
class native_simd;

template <>
class native_simd<uint64_t> {
public:
    static constexpr int alignment = 16;
    static const int size = 2;
    __m128i xmm;

    native_simd() noexcept
    {}

    native_simd(__m128i val) noexcept : xmm(val)
    {}

    native_simd(uint64_t a) noexcept
    {
        xmm = _mm_set1_epi64x(static_cast<int64_t>(a));
    }

    native_simd(const uint64_t* p) noexcept
    {
        load(p);
    }

    operator __m128i() const noexcept
    {
        return xmm;
    }

    native_simd load(const uint64_t* p) noexcept
    {
        xmm = _mm_set_epi64x(static_cast<int64_t>(p[1]), static_cast<int64_t>(p[0]));
        return *this;
    }

    void store(uint64_t* p) const noexcept
    {
        _mm_store_si128(reinterpret_cast<__m128i*>(p), xmm);
    }

    native_simd operator+(const native_simd b) const noexcept
    {
        return _mm_add_epi64(xmm, b);
    }

    native_simd& operator+=(const native_simd b) noexcept
    {
        xmm = _mm_add_epi64(xmm, b);
        return *this;
    }

    native_simd operator-(const native_simd b) const noexcept
    {
        return _mm_sub_epi64(xmm, b);
    }

    native_simd operator-() const noexcept
    {
        return _mm_sub_epi64(_mm_setzero_si128(), xmm);
    }

    native_simd& operator-=(const native_simd b) noexcept
    {
        xmm = _mm_sub_epi64(xmm, b);
        return *this;
    }
};

template <>
class native_simd<uint32_t> {
public:
    static constexpr int alignment = 16;
    static const int size = 4;
    __m128i xmm;

    native_simd() noexcept
    {}

    native_simd(__m128i val) noexcept : xmm(val)
    {}

    native_simd(uint32_t a) noexcept
    {
        xmm = _mm_set1_epi32(static_cast<int>(a));
    }

    native_simd(const uint64_t* p) noexcept
    {
        load(p);
    }

    operator __m128i() const noexcept
    {
        return xmm;
    }

    native_simd load(const uint64_t* p) noexcept
    {
        xmm = _mm_set_epi64x(static_cast<int64_t>(p[1]), static_cast<int64_t>(p[0]));
        return *this;
    }

    void store(uint32_t* p) const noexcept
    {
        _mm_store_si128(reinterpret_cast<__m128i*>(p), xmm);
    }

    native_simd operator+(const native_simd b) const noexcept
    {
        return _mm_add_epi32(xmm, b);
    }

    native_simd& operator+=(const native_simd b) noexcept
    {
        xmm = _mm_add_epi32(xmm, b);
        return *this;
    }

    native_simd operator-(const native_simd b) const noexcept
    {
        return _mm_sub_epi32(xmm, b);
    }

    native_simd operator-() const noexcept
    {
        return _mm_sub_epi32(_mm_setzero_si128(), xmm);
    }

    native_simd& operator-=(const native_simd b) noexcept
    {
        xmm = _mm_sub_epi32(xmm, b);
        return *this;
    }
};

template <>
class native_simd<uint16_t> {
public:
    static constexpr int alignment = 16;
    static const int size = 8;
    __m128i xmm;

    native_simd() noexcept
    {}

    native_simd(__m128i val) noexcept : xmm(val)
    {}

    native_simd(uint16_t a) noexcept
    {
        xmm = _mm_set1_epi16(static_cast<short>(a));
    }

    native_simd(const uint64_t* p) noexcept
    {
        load(p);
    }

    operator __m128i() const noexcept
    {
        return xmm;
    }

    native_simd load(const uint64_t* p) noexcept
    {
        xmm = _mm_set_epi64x(static_cast<int64_t>(p[1]), static_cast<int64_t>(p[0]));
        return *this;
    }

    void store(uint16_t* p) const noexcept
    {
        _mm_store_si128(reinterpret_cast<__m128i*>(p), xmm);
    }

    native_simd operator+(const native_simd b) const noexcept
    {
        return _mm_add_epi16(xmm, b);
    }

    native_simd& operator+=(const native_simd b) noexcept
    {
        xmm = _mm_add_epi16(xmm, b);
        return *this;
    }

    native_simd operator-(const native_simd b) const noexcept
    {
        return _mm_sub_epi16(xmm, b);
    }

    native_simd operator-() const noexcept
    {
        return _mm_sub_epi16(_mm_setzero_si128(), xmm);
    }

    native_simd& operator-=(const native_simd b) noexcept
    {
        xmm = _mm_sub_epi16(xmm, b);
        return *this;
    }
};

template <>
class native_simd<uint8_t> {
public:
    static constexpr int alignment = 16;
    static const int size = 16;
    __m128i xmm;

    native_simd() noexcept
    {}

    native_simd(__m128i val) noexcept : xmm(val)
    {}

    native_simd(uint8_t a) noexcept
    {
        xmm = _mm_set1_epi8(static_cast<char>(a));
    }

    native_simd(const uint64_t* p) noexcept
    {
        load(p);
    }

    operator __m128i() const noexcept
    {
        return xmm;
    }

    native_simd load(const uint64_t* p) noexcept
    {
        xmm = _mm_set_epi64x(static_cast<int64_t>(p[1]), static_cast<int64_t>(p[0]));
        return *this;
    }

    void store(uint8_t* p) const noexcept
    {
        _mm_store_si128(reinterpret_cast<__m128i*>(p), xmm);
    }

    native_simd operator+(const native_simd b) const noexcept
    {
        return _mm_add_epi8(xmm, b);
    }

    native_simd& operator+=(const native_simd b) noexcept
    {
        xmm = _mm_add_epi8(xmm, b);
        return *this;
    }

    native_simd operator-(const native_simd b) const noexcept
    {
        return _mm_sub_epi8(xmm, b);
    }

    native_simd operator-() const noexcept
    {
        return _mm_sub_epi8(_mm_setzero_si128(), xmm);
    }

    native_simd& operator-=(const native_simd b) noexcept
    {
        xmm = _mm_sub_epi8(xmm, b);
        return *this;
    }
};

template <typename T>
std::ostream& operator<<(std::ostream& os, const native_simd<T>& a)
{
    alignas(native_simd<T>::alignment) std::array<T, native_simd<T>::size> res;
    a.store(&res[0]);

    for (size_t i = res.size() - 1; i != 0; i--)
        os << std::bitset<std::numeric_limits<T>::digits>(res[i]) << "|";

    os << std::bitset<std::numeric_limits<T>::digits>(res[0]);
    return os;
}

template <typename T>
__m128i hadd_impl(__m128i x) noexcept;

template <>
inline __m128i hadd_impl<uint8_t>(__m128i x) noexcept
{
    return x;
}

template <>
inline __m128i hadd_impl<uint16_t>(__m128i x) noexcept
{
    const __m128i mask = _mm_set1_epi16(0x001f);
    __m128i y = _mm_srli_si128(x, 1);
    x = _mm_add_epi16(x, y);
    return _mm_and_si128(x, mask);
}

template <>
inline __m128i hadd_impl<uint32_t>(__m128i x) noexcept
{
    const __m128i mask = _mm_set1_epi32(0x0000003f);
    x = hadd_impl<uint16_t>(x);
    __m128i y = _mm_srli_si128(x, 2);
    x = _mm_add_epi32(x, y);
    return _mm_and_si128(x, mask);
}

template <>
inline __m128i hadd_impl<uint64_t>(__m128i x) noexcept
{
    return _mm_sad_epu8(x, _mm_setzero_si128());
}

template <typename T>
native_simd<T> popcount_impl(const native_simd<T>& v) noexcept
{
    const __m128i m1 = _mm_set1_epi8(0x55);
    const __m128i m2 = _mm_set1_epi8(0x33);
    const __m128i m3 = _mm_set1_epi8(0x0F);

    /* Note: if we returned x here it would be like _mm_popcnt_epi1(x) */
    __m128i y;
    __m128i x = v;
    /* add even and odd bits*/
    y = _mm_srli_epi64(x, 1); // put even bits in odd place
    y = _mm_and_si128(y, m1); // mask out the even bits (0x55)
    x = _mm_subs_epu8(x, y);  // shortcut to mask even bits and add
    /* if we just returned x here it would be like popcnt_epi2(x) */
    /* now add the half nibbles */
    y = _mm_srli_epi64(x, 2); // move half nibbles in place to add
    y = _mm_and_si128(y, m2); // mask off the extra half nibbles (0x0f)
    x = _mm_and_si128(x, m2); // ditto
    x = _mm_adds_epu8(x, y);  // totals are a maximum of 5 bits (0x1f)
    /* if we just returned x here it would be like popcnt_epi4(x) */
    /* now add the nibbles */
    y = _mm_srli_epi64(x, 4); // move nibbles in place to add
    x = _mm_adds_epu8(x, y);  // totals are a maximum of 6 bits (0x3f)
    x = _mm_and_si128(x, m3); // mask off the extra bits

    /* todo use when sse3 available
    __m128i lookup = _mm_setr_epi8(0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
    const __m128i low_mask = _mm_set1_epi8(0x0F);
    __m128i lo = _mm_and_si128(v, low_mask);
    __m128i hi = _mm_and_si256(_mm_srli_epi32(v, 4), low_mask);
    __m128i popcnt1 = _mm_shuffle_epi8(lookup, lo);
    __m128i popcnt2 = _mm_shuffle_epi8(lookup, hi);
    __m128i total = _mm_add_epi8(popcnt1, popcnt2);*/

    return hadd_impl<T>(x);
}

template <typename T>
std::array<T, native_simd<T>::size> popcount(const native_simd<T>& a) noexcept
{
    alignas(native_simd<T>::alignment) std::array<T, native_simd<T>::size> res;
    popcount_impl(a).store(&res[0]);
    return res;
}

// function andnot: a & ~ b
template <typename T>
native_simd<T> andnot(const native_simd<T>& a, const native_simd<T>& b)
{
    return _mm_andnot_si128(b, a);
}

static inline native_simd<uint8_t> operator==(const native_simd<uint8_t>& a,
                                              const native_simd<uint8_t>& b) noexcept
{
    return _mm_cmpeq_epi8(a, b);
}

static inline native_simd<uint16_t> operator==(const native_simd<uint16_t>& a,
                                               const native_simd<uint16_t>& b) noexcept
{
    return _mm_cmpeq_epi16(a, b);
}

static inline native_simd<uint32_t> operator==(const native_simd<uint32_t>& a,
                                               const native_simd<uint32_t>& b) noexcept
{
    return _mm_cmpeq_epi32(a, b);
}

static inline native_simd<uint64_t> operator==(const native_simd<uint64_t>& a,
                                               const native_simd<uint64_t>& b) noexcept
{
    // no 64 compare instruction. Do two 32 bit compares
    __m128i com32 = _mm_cmpeq_epi32(a, b);           // 32 bit compares
    __m128i com32s = _mm_shuffle_epi32(com32, 0xB1); // swap low and high dwords
    __m128i test = _mm_and_si128(com32, com32s);     // low & high
    __m128i teste = _mm_srai_epi32(test, 31);        // extend sign bit to 32 bits
    __m128i testee = _mm_shuffle_epi32(teste, 0xF5); // extend sign bit to 64 bits
    return testee;
}

template <typename T>
static inline native_simd<T> operator!=(const native_simd<T>& a, const native_simd<T>& b) noexcept
{
    return ~(a == b);
}

static inline native_simd<uint8_t> operator<<(const native_simd<uint8_t>& a, int b) noexcept
{
    char mask = static_cast<char>(0xFF >> b);
    __m128i am = _mm_and_si128(a, _mm_set1_epi8(mask));
    return _mm_slli_epi16(am, b);
}

static inline native_simd<uint16_t> operator<<(const native_simd<uint16_t>& a, int b) noexcept
{
    return _mm_slli_epi16(a, b);
}

static inline native_simd<uint32_t> operator<<(const native_simd<uint32_t>& a, int b) noexcept
{
    return _mm_slli_epi32(a, b);
}

static inline native_simd<uint64_t> operator<<(const native_simd<uint64_t>& a, int b) noexcept
{
    return _mm_slli_epi64(a, b);
}

static inline native_simd<uint8_t> operator>>(const native_simd<uint8_t>& a, int b) noexcept
{
    char mask = static_cast<char>(0xFF << b);
    __m128i am = _mm_and_si128(a, _mm_set1_epi8(mask));
    return _mm_srli_epi16(am, b);
}

static inline native_simd<uint16_t> operator>>(const native_simd<uint16_t>& a, int b) noexcept
{
    return _mm_srli_epi16(a, b);
}

static inline native_simd<uint32_t> operator>>(const native_simd<uint32_t>& a, int b) noexcept
{
    return _mm_srli_epi32(a, b);
}

static inline native_simd<uint64_t> operator>>(const native_simd<uint64_t>& a, int b) noexcept
{
    return _mm_srli_epi64(a, b);
}

template <typename T>
native_simd<T> operator&(const native_simd<T>& a, const native_simd<T>& b) noexcept
{
    return _mm_and_si128(a, b);
}

template <typename T>
native_simd<T> operator&=(native_simd<T>& a, const native_simd<T>& b) noexcept
{
    a = a & b;
    return a;
}

template <typename T>
native_simd<T> operator|(const native_simd<T>& a, const native_simd<T>& b) noexcept
{
    return _mm_or_si128(a, b);
}

template <typename T>
native_simd<T> operator|=(native_simd<T>& a, const native_simd<T>& b) noexcept
{
    a = a | b;
    return a;
}

template <typename T>
native_simd<T> operator^(const native_simd<T>& a, const native_simd<T>& b) noexcept
{
    return _mm_xor_si128(a, b);
}

template <typename T>
native_simd<T> operator^=(native_simd<T>& a, const native_simd<T>& b) noexcept
{
    a = a ^ b;
    return a;
}

template <typename T>
native_simd<T> operator~(const native_simd<T>& a) noexcept
{
    return _mm_xor_si128(a, _mm_set1_epi32(-1));
}

// potentially we want a special native_simd<bool> for this
static inline native_simd<uint8_t> operator>=(const native_simd<uint8_t>& a,
                                              const native_simd<uint8_t>& b) noexcept
{
    return _mm_cmpeq_epi8(_mm_max_epu8(a, b), a); // a == max(a,b)
}

static inline native_simd<uint16_t> operator>=(const native_simd<uint16_t>& a,
                                               const native_simd<uint16_t>& b) noexcept
{
    /* sse4.1 */
#if 0
    return _mm_cmpeq_epi16(_mm_max_epu16(a, b), a); // a == max(a,b)
#endif

    __m128i s = _mm_subs_epu16(b, a);               // b-a, saturated
    return _mm_cmpeq_epi16(s, _mm_setzero_si128()); // s == 0
}

static inline native_simd<uint64_t> operator>(const native_simd<uint64_t>& a,
                                              const native_simd<uint64_t>& b) noexcept;
static inline native_simd<uint32_t> operator>(const native_simd<uint32_t>& a,
                                              const native_simd<uint32_t>& b) noexcept;

static inline native_simd<uint32_t> operator>=(const native_simd<uint32_t>& a,
                                               const native_simd<uint32_t>& b) noexcept
{
    /* sse4.1 */
#if 0
    return (Vec4ib)_mm_cmpeq_epi32(_mm_max_epu32(a, b), a); // a == max(a,b)
#endif

    return ~(b > a);
}

static inline native_simd<uint64_t> operator>=(const native_simd<uint64_t>& a,
                                               const native_simd<uint64_t>& b) noexcept
{
    return ~(b > a);
}

template <typename T>
static inline native_simd<T> operator<=(const native_simd<T>& a, const native_simd<T>& b) noexcept
{
    return b >= a;
}

static inline native_simd<uint8_t> operator>(const native_simd<uint8_t>& a,
                                             const native_simd<uint8_t>& b) noexcept
{
    return ~(b >= a);
}

static inline native_simd<uint16_t> operator>(const native_simd<uint16_t>& a,
                                              const native_simd<uint16_t>& b) noexcept
{
    return ~(b >= a);
}

static inline native_simd<uint32_t> operator>(const native_simd<uint32_t>& a,
                                              const native_simd<uint32_t>& b) noexcept
{
    __m128i signbit = _mm_set1_epi32(static_cast<int32_t>(0x80000000));
    __m128i a1 = _mm_xor_si128(a, signbit);
    __m128i b1 = _mm_xor_si128(b, signbit);
    return _mm_cmpgt_epi32(a1, b1); // signed compare
}

static inline native_simd<uint64_t> operator>(const native_simd<uint64_t>& a,
                                              const native_simd<uint64_t>& b) noexcept
{
    __m128i sign32 = _mm_set1_epi32(static_cast<int32_t>(0x80000000)); // sign bit of each dword
    __m128i aflip = _mm_xor_si128(a, sign32);          // a with sign bits flipped to use signed compare
    __m128i bflip = _mm_xor_si128(b, sign32);          // b with sign bits flipped to use signed compare
    __m128i equal = _mm_cmpeq_epi32(a, b);             // a == b, dwords
    __m128i bigger = _mm_cmpgt_epi32(aflip, bflip);    // a > b, dwords
    __m128i biggerl = _mm_shuffle_epi32(bigger, 0xA0); // a > b, low dwords copied to high dwords
    __m128i eqbig = _mm_and_si128(equal, biggerl);     // high part equal and low part bigger
    __m128i hibig = _mm_or_si128(bigger, eqbig);  // high part bigger or high part equal and low part bigger
    __m128i big = _mm_shuffle_epi32(hibig, 0xF5); // result copied to low part
    return big;
}

template <typename T>
static inline native_simd<T> operator<(const native_simd<T>& a, const native_simd<T>& b) noexcept
{
    return b > a;
}

} // namespace simd_sse2
} // namespace detail
} // namespace duckdb_rapidfuzz
