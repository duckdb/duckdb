// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// From Apache Impala as of 2016-01-29. Pared down to a minimal set of
// functions needed for parquet-cpp

#ifndef ARROW_UTIL_SSE_UTIL_H
#define ARROW_UTIL_SSE_UTIL_H

#undef ARROW_HAVE_SSE2
#undef ARROW_HAVE_SSE4_2

#ifdef ARROW_USE_SIMD

// MSVC x86-64

#if (defined(_M_AMD64) || defined(_M_X64))
#define ARROW_HAVE_SSE2 1
#define ARROW_HAVE_SSE4_2 1
#include <intrin.h>
#endif

// gcc/clang (possibly others)

#if defined(__SSE4_2__)
#define ARROW_HAVE_SSE2 1
#include <emmintrin.h>
#endif

#if defined(__SSE4_2__)
#define ARROW_HAVE_SSE4_2 1
#include <nmmintrin.h>
#endif

#endif

namespace arrow {

/// This class contains constants useful for text processing with SSE4.2 intrinsics.
namespace SSEUtil {
/// Number of characters that fit in 64/128 bit register.  SSE provides instructions
/// for loading 64 or 128 bits into a register at a time.
static const int CHARS_PER_64_BIT_REGISTER = 8;
static const int CHARS_PER_128_BIT_REGISTER = 16;

/// SSE4.2 adds instructions for text processing.  The instructions have a control
/// byte that determines some of functionality of the instruction.  (Equivalent to
/// GCC's _SIDD_CMP_EQUAL_ANY, etc).
static const int PCMPSTR_EQUAL_ANY = 0x00;     // strchr
static const int PCMPSTR_EQUAL_EACH = 0x08;    // strcmp
static const int PCMPSTR_UBYTE_OPS = 0x00;     // unsigned char (8-bits, rather than 16)
static const int PCMPSTR_NEG_POLARITY = 0x10;  // see Intel SDM chapter 4.1.4.

/// In this mode, SSE text processing functions will return a mask of all the
/// characters that matched.
static const int STRCHR_MODE = PCMPSTR_EQUAL_ANY | PCMPSTR_UBYTE_OPS;

/// In this mode, SSE text processing functions will return the number of
/// bytes that match consecutively from the beginning.
static const int STRCMP_MODE =
    PCMPSTR_EQUAL_EACH | PCMPSTR_UBYTE_OPS | PCMPSTR_NEG_POLARITY;

/// Precomputed mask values up to 16 bits.
static const int SSE_BITMASK[CHARS_PER_128_BIT_REGISTER] = {
    1 << 0, 1 << 1, 1 << 2,  1 << 3,  1 << 4,  1 << 5,  1 << 6,  1 << 7,
    1 << 8, 1 << 9, 1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15,
};
}  // namespace SSEUtil

#ifdef ARROW_HAVE_SSE4_2

/// Define the SSE 4.2 intrinsics.  The caller must first verify at runtime (or codegen
/// IR load time) that the processor supports SSE 4.2 before calling these.  These are
/// defined outside the namespace because the IR w/ SSE 4.2 case needs to use macros.

template <int MODE>
static inline __m128i SSE4_cmpestrm(__m128i str1, int len1, __m128i str2, int len2) {
  return _mm_cmpestrm(str1, len1, str2, len2, MODE);
}

template <int MODE>
static inline int SSE4_cmpestri(__m128i str1, int len1, __m128i str2, int len2) {
  return _mm_cmpestri(str1, len1, str2, len2, MODE);
}

static inline uint32_t SSE4_crc32_u8(uint32_t crc, uint8_t v) {
  return _mm_crc32_u8(crc, v);
}

static inline uint32_t SSE4_crc32_u16(uint32_t crc, uint16_t v) {
  return _mm_crc32_u16(crc, v);
}

static inline uint32_t SSE4_crc32_u32(uint32_t crc, uint32_t v) {
  return _mm_crc32_u32(crc, v);
}

static inline uint32_t SSE4_crc32_u64(uint32_t crc, uint64_t v) {
  return static_cast<uint32_t>(_mm_crc32_u64(crc, v));
}

#else  // without SSE 4.2.

// __m128i may not be defined, so deduce it with a template parameter
template <int MODE, typename __m128i>
static inline __m128i SSE4_cmpestrm(__m128i str1, int len1, __m128i str2, int len2) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return (__m128i){0};  // NOLINT
}

template <int MODE, typename __m128i>
static inline int SSE4_cmpestri(__m128i str1, int len1, __m128i str2, int len2) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return 0;
}

static inline uint32_t SSE4_crc32_u8(uint32_t, uint8_t) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

static inline uint32_t SSE4_crc32_u16(uint32_t, uint16_t) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

static inline uint32_t SSE4_crc32_u32(uint32_t, uint32_t) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

static inline uint32_t SSE4_crc32_u64(uint32_t, uint64_t) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

#endif  // ARROW_HAVE_SSE4_2

}  // namespace arrow

#endif  //  ARROW_UTIL_SSE_UTIL_H
