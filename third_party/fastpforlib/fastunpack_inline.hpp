/**
 * Inlined, compile-time-unrolled bitunpacking primitives.
 *
 * Three layers (all header-only for full inlining at -O3):
 *   fastunpack32_inline<W, OutT>(in, out)           — 32-value kernel, W compile-time.
 *   fastunpack_N<W, OutT>(in, out, count)           — loop over 32-value groups; W compile-time.
 *   fastunpack_bulk<OutT>(in, out, count, width)    — ONE switch on width; count runtime.
 *
 * Input is always read as `const uint32_t *` (the bitpacking format stores
 * packed bits as 32-bit words). Output is any integer OutT ∈
 * {uint8_t, uint16_t, uint32_t, uint64_t}. Constraint: W ≤ sizeof(OutT)*8.
 *
 * count must be a multiple of 32 (the format's algorithm-group size). The
 * primitive unconditionally reads 4*W bytes per 32-value group and writes
 * 32 OutT values. Callers that want fewer output values use a scratch buffer
 * plus memcpy (the existing pattern — unchanged by this header).
 */
#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <type_traits>
#include <utility>
#include "bitpackinghelpers.h"

#if defined(__GNUC__) || defined(__clang__)
#define FASTUNPACK_ALWAYS_INLINE __attribute__((always_inline)) inline
#else
#define FASTUNPACK_ALWAYS_INLINE inline
#endif

namespace duckdb_fastpforlib {

//===--------------------------------------------------------------------===//
// Layer 1: 32-value kernel
//===--------------------------------------------------------------------===//

namespace fastunpack_internal {

// Extract the I-th value of width W from the packed uint32_t stream.
// All shifts / masks / word indices are compile-time constants.
template <uint32_t W, class OutT, std::size_t I>
static FASTUNPACK_ALWAYS_INLINE void extract_one(const uint32_t *__restrict in, OutT *__restrict out) {
	constexpr std::size_t bit_pos = std::size_t(I) * std::size_t(W);
	constexpr std::size_t word = bit_pos / 32;
	constexpr uint32_t shift = uint32_t(bit_pos % 32);

	if constexpr (W <= 32) {
		constexpr uint32_t mask = (W == 32) ? ~uint32_t(0) : ((uint32_t(1) << (W & 31)) - 1);
		if constexpr (shift + W <= 32) {
			// No straddle.
			out[I] = static_cast<OutT>((in[word] >> shift) & mask);
		} else {
			// Straddles two words. shift >= 1 is guaranteed here
			// (shift + W > 32 with W ≤ 32 ⇒ shift ≥ 1), so (32 - shift)
			// is in [1, 31] and safe.
			const uint32_t v = (in[word] >> shift) | (in[word + 1] << (32 - shift));
			out[I] = static_cast<OutT>(v & mask);
		}
	} else {
		// W > 32: OutT must be uint64_t. Up to 3 words may be needed.
		constexpr uint64_t mask = (W == 64) ? ~uint64_t(0) : ((uint64_t(1) << (W & 63)) - 1);
		if constexpr (shift == 0) {
			// bit_pos aligned to a word boundary: exactly 2 words needed
			// for W ≤ 64 (W=64 at I=0,1,... always has shift=0).
			const uint64_t v = uint64_t(in[word]) | (uint64_t(in[word + 1]) << 32);
			out[I] = static_cast<OutT>(v & mask);
		} else {
			uint64_t v = uint64_t(in[word]) >> shift;
			v |= uint64_t(in[word + 1]) << (32 - shift);
			if constexpr (shift + W > 64) {
				v |= uint64_t(in[word + 2]) << (64 - shift);
			}
			out[I] = static_cast<OutT>(v & mask);
		}
	}
}

template <uint32_t W, class OutT, std::size_t... Is>
static FASTUNPACK_ALWAYS_INLINE void fastunpack32_impl(const uint32_t *__restrict in, OutT *__restrict out,
                                                       std::index_sequence<Is...>) {
	(extract_one<W, OutT, Is>(in, out), ...);
}

} // namespace fastunpack_internal

//! Unpack 32 values of compile-time width W from the packed uint32_t stream `in`
//! into `out`. Fully unrolled: 32 constant-folded extract statements.
template <uint32_t W, class OutT>
FASTUNPACK_ALWAYS_INLINE void fastunpack32_inline(const uint32_t *__restrict in, OutT *__restrict out) {
	if constexpr (W == 0) {
		std::memset(out, 0, 32 * sizeof(OutT));
	} else if constexpr (W == 4 && std::is_same<OutT, uint8_t>::value) {
		auto src = reinterpret_cast<const uint8_t *>(in);
		internal::__fastunpack4(src, out);
		internal::__fastunpack4(src + 4, out + 8);
		internal::__fastunpack4(src + 8, out + 16);
		internal::__fastunpack4(src + 12, out + 24);
	} else if constexpr (W == 12 && std::is_same<OutT, uint16_t>::value) {
		auto src = reinterpret_cast<const uint16_t *>(in);
		internal::__fastunpack12(src, out);
		internal::__fastunpack12(src + 12, out + 16);
	} else if constexpr (W == 24 && std::is_same<OutT, uint32_t>::value) {
		internal::__fastunpack24(in, out);
	} else if constexpr (W == 8 * sizeof(OutT)) {
		std::memcpy(out, in, 32 * sizeof(OutT));
	} else {
		fastunpack_internal::fastunpack32_impl<W, OutT>(in, out, std::make_index_sequence<32>{});
	}
}

//===--------------------------------------------------------------------===//
// Layer 2: count-parameterised loop over 32-value groups
//===--------------------------------------------------------------------===//

//! Unpack `count` values (must be a multiple of 32) of compile-time width W.
//! W is a template parameter so the kernel inlines with constant shifts/masks;
//! count is runtime but iterations are independent so clang vectorises /
//! interleaves across consecutive 32-value groups.
template <uint32_t W, class OutT>
FASTUNPACK_ALWAYS_INLINE void fastunpack_N(const uint32_t *__restrict in, OutT *__restrict out, std::size_t count) {
	const std::size_t groups = count / 32;
	for (std::size_t g = 0; g < groups; g++) {
		// Each group consumes W uint32_t words of packed input
		// (32 × W bits = 4W bytes = W uint32_t's) and writes 32 OutT values.
		fastunpack32_inline<W, OutT>(in + g * W, out + g * 32);
	}
}

//===--------------------------------------------------------------------===//
// Layer 3: single switch on width
//===--------------------------------------------------------------------===//

//! One switch(width) per call. The selected case calls fastunpack_N<W, OutT>
//! with W as a compile-time constant, so the body sees constant shifts/masks
//! across the whole `count`-long run.
template <class OutT>
inline void fastunpack_bulk(const uint32_t *__restrict in, OutT *__restrict out,
                            std::size_t count, uint32_t width);

// uint8_t output: widths 0..8.
template <>
inline void fastunpack_bulk<uint8_t>(const uint32_t *__restrict in, uint8_t *__restrict out,
                                     std::size_t count, uint32_t width) {
	switch (width) {
	case 0: fastunpack_N<0,  uint8_t>(in, out, count); return;
	case 1: fastunpack_N<1,  uint8_t>(in, out, count); return;
	case 2: fastunpack_N<2,  uint8_t>(in, out, count); return;
	case 3: fastunpack_N<3,  uint8_t>(in, out, count); return;
	case 4: fastunpack_N<4,  uint8_t>(in, out, count); return;
	case 5: fastunpack_N<5,  uint8_t>(in, out, count); return;
	case 6: fastunpack_N<6,  uint8_t>(in, out, count); return;
	case 7: fastunpack_N<7,  uint8_t>(in, out, count); return;
	case 8: fastunpack_N<8,  uint8_t>(in, out, count); return;
	default: __builtin_unreachable();
	}
}

// uint16_t output: widths 0..16.
template <>
inline void fastunpack_bulk<uint16_t>(const uint32_t *__restrict in, uint16_t *__restrict out,
                                      std::size_t count, uint32_t width) {
	switch (width) {
	case 0:  fastunpack_N<0,  uint16_t>(in, out, count); return;
	case 1:  fastunpack_N<1,  uint16_t>(in, out, count); return;
	case 2:  fastunpack_N<2,  uint16_t>(in, out, count); return;
	case 3:  fastunpack_N<3,  uint16_t>(in, out, count); return;
	case 4:  fastunpack_N<4,  uint16_t>(in, out, count); return;
	case 5:  fastunpack_N<5,  uint16_t>(in, out, count); return;
	case 6:  fastunpack_N<6,  uint16_t>(in, out, count); return;
	case 7:  fastunpack_N<7,  uint16_t>(in, out, count); return;
	case 8:  fastunpack_N<8,  uint16_t>(in, out, count); return;
	case 9:  fastunpack_N<9,  uint16_t>(in, out, count); return;
	case 10: fastunpack_N<10, uint16_t>(in, out, count); return;
	case 11: fastunpack_N<11, uint16_t>(in, out, count); return;
	case 12: fastunpack_N<12, uint16_t>(in, out, count); return;
	case 13: fastunpack_N<13, uint16_t>(in, out, count); return;
	case 14: fastunpack_N<14, uint16_t>(in, out, count); return;
	case 15: fastunpack_N<15, uint16_t>(in, out, count); return;
	case 16: fastunpack_N<16, uint16_t>(in, out, count); return;
	default: __builtin_unreachable();
	}
}

// uint32_t output: widths 0..32.
template <>
inline void fastunpack_bulk<uint32_t>(const uint32_t *__restrict in, uint32_t *__restrict out,
                                      std::size_t count, uint32_t width) {
	switch (width) {
	case 0:  fastunpack_N<0,  uint32_t>(in, out, count); return;
	case 1:  fastunpack_N<1,  uint32_t>(in, out, count); return;
	case 2:  fastunpack_N<2,  uint32_t>(in, out, count); return;
	case 3:  fastunpack_N<3,  uint32_t>(in, out, count); return;
	case 4:  fastunpack_N<4,  uint32_t>(in, out, count); return;
	case 5:  fastunpack_N<5,  uint32_t>(in, out, count); return;
	case 6:  fastunpack_N<6,  uint32_t>(in, out, count); return;
	case 7:  fastunpack_N<7,  uint32_t>(in, out, count); return;
	case 8:  fastunpack_N<8,  uint32_t>(in, out, count); return;
	case 9:  fastunpack_N<9,  uint32_t>(in, out, count); return;
	case 10: fastunpack_N<10, uint32_t>(in, out, count); return;
	case 11: fastunpack_N<11, uint32_t>(in, out, count); return;
	case 12: fastunpack_N<12, uint32_t>(in, out, count); return;
	case 13: fastunpack_N<13, uint32_t>(in, out, count); return;
	case 14: fastunpack_N<14, uint32_t>(in, out, count); return;
	case 15: fastunpack_N<15, uint32_t>(in, out, count); return;
	case 16: fastunpack_N<16, uint32_t>(in, out, count); return;
	case 17: fastunpack_N<17, uint32_t>(in, out, count); return;
	case 18: fastunpack_N<18, uint32_t>(in, out, count); return;
	case 19: fastunpack_N<19, uint32_t>(in, out, count); return;
	case 20: fastunpack_N<20, uint32_t>(in, out, count); return;
	case 21: fastunpack_N<21, uint32_t>(in, out, count); return;
	case 22: fastunpack_N<22, uint32_t>(in, out, count); return;
	case 23: fastunpack_N<23, uint32_t>(in, out, count); return;
	case 24: fastunpack_N<24, uint32_t>(in, out, count); return;
	case 25: fastunpack_N<25, uint32_t>(in, out, count); return;
	case 26: fastunpack_N<26, uint32_t>(in, out, count); return;
	case 27: fastunpack_N<27, uint32_t>(in, out, count); return;
	case 28: fastunpack_N<28, uint32_t>(in, out, count); return;
	case 29: fastunpack_N<29, uint32_t>(in, out, count); return;
	case 30: fastunpack_N<30, uint32_t>(in, out, count); return;
	case 31: fastunpack_N<31, uint32_t>(in, out, count); return;
	case 32: fastunpack_N<32, uint32_t>(in, out, count); return;
	default: __builtin_unreachable();
	}
}

// uint64_t output: widths 0..64.
template <>
inline void fastunpack_bulk<uint64_t>(const uint32_t *__restrict in, uint64_t *__restrict out,
                                      std::size_t count, uint32_t width) {
	switch (width) {
	case 0:  fastunpack_N<0,  uint64_t>(in, out, count); return;
	case 1:  fastunpack_N<1,  uint64_t>(in, out, count); return;
	case 2:  fastunpack_N<2,  uint64_t>(in, out, count); return;
	case 3:  fastunpack_N<3,  uint64_t>(in, out, count); return;
	case 4:  fastunpack_N<4,  uint64_t>(in, out, count); return;
	case 5:  fastunpack_N<5,  uint64_t>(in, out, count); return;
	case 6:  fastunpack_N<6,  uint64_t>(in, out, count); return;
	case 7:  fastunpack_N<7,  uint64_t>(in, out, count); return;
	case 8:  fastunpack_N<8,  uint64_t>(in, out, count); return;
	case 9:  fastunpack_N<9,  uint64_t>(in, out, count); return;
	case 10: fastunpack_N<10, uint64_t>(in, out, count); return;
	case 11: fastunpack_N<11, uint64_t>(in, out, count); return;
	case 12: fastunpack_N<12, uint64_t>(in, out, count); return;
	case 13: fastunpack_N<13, uint64_t>(in, out, count); return;
	case 14: fastunpack_N<14, uint64_t>(in, out, count); return;
	case 15: fastunpack_N<15, uint64_t>(in, out, count); return;
	case 16: fastunpack_N<16, uint64_t>(in, out, count); return;
	case 17: fastunpack_N<17, uint64_t>(in, out, count); return;
	case 18: fastunpack_N<18, uint64_t>(in, out, count); return;
	case 19: fastunpack_N<19, uint64_t>(in, out, count); return;
	case 20: fastunpack_N<20, uint64_t>(in, out, count); return;
	case 21: fastunpack_N<21, uint64_t>(in, out, count); return;
	case 22: fastunpack_N<22, uint64_t>(in, out, count); return;
	case 23: fastunpack_N<23, uint64_t>(in, out, count); return;
	case 24: fastunpack_N<24, uint64_t>(in, out, count); return;
	case 25: fastunpack_N<25, uint64_t>(in, out, count); return;
	case 26: fastunpack_N<26, uint64_t>(in, out, count); return;
	case 27: fastunpack_N<27, uint64_t>(in, out, count); return;
	case 28: fastunpack_N<28, uint64_t>(in, out, count); return;
	case 29: fastunpack_N<29, uint64_t>(in, out, count); return;
	case 30: fastunpack_N<30, uint64_t>(in, out, count); return;
	case 31: fastunpack_N<31, uint64_t>(in, out, count); return;
	case 32: fastunpack_N<32, uint64_t>(in, out, count); return;
	case 33: fastunpack_N<33, uint64_t>(in, out, count); return;
	case 34: fastunpack_N<34, uint64_t>(in, out, count); return;
	case 35: fastunpack_N<35, uint64_t>(in, out, count); return;
	case 36: fastunpack_N<36, uint64_t>(in, out, count); return;
	case 37: fastunpack_N<37, uint64_t>(in, out, count); return;
	case 38: fastunpack_N<38, uint64_t>(in, out, count); return;
	case 39: fastunpack_N<39, uint64_t>(in, out, count); return;
	case 40: fastunpack_N<40, uint64_t>(in, out, count); return;
	case 41: fastunpack_N<41, uint64_t>(in, out, count); return;
	case 42: fastunpack_N<42, uint64_t>(in, out, count); return;
	case 43: fastunpack_N<43, uint64_t>(in, out, count); return;
	case 44: fastunpack_N<44, uint64_t>(in, out, count); return;
	case 45: fastunpack_N<45, uint64_t>(in, out, count); return;
	case 46: fastunpack_N<46, uint64_t>(in, out, count); return;
	case 47: fastunpack_N<47, uint64_t>(in, out, count); return;
	case 48: fastunpack_N<48, uint64_t>(in, out, count); return;
	case 49: fastunpack_N<49, uint64_t>(in, out, count); return;
	case 50: fastunpack_N<50, uint64_t>(in, out, count); return;
	case 51: fastunpack_N<51, uint64_t>(in, out, count); return;
	case 52: fastunpack_N<52, uint64_t>(in, out, count); return;
	case 53: fastunpack_N<53, uint64_t>(in, out, count); return;
	case 54: fastunpack_N<54, uint64_t>(in, out, count); return;
	case 55: fastunpack_N<55, uint64_t>(in, out, count); return;
	case 56: fastunpack_N<56, uint64_t>(in, out, count); return;
	case 57: fastunpack_N<57, uint64_t>(in, out, count); return;
	case 58: fastunpack_N<58, uint64_t>(in, out, count); return;
	case 59: fastunpack_N<59, uint64_t>(in, out, count); return;
	case 60: fastunpack_N<60, uint64_t>(in, out, count); return;
	case 61: fastunpack_N<61, uint64_t>(in, out, count); return;
	case 62: fastunpack_N<62, uint64_t>(in, out, count); return;
	case 63: fastunpack_N<63, uint64_t>(in, out, count); return;
	case 64: fastunpack_N<64, uint64_t>(in, out, count); return;
	default: __builtin_unreachable();
	}
}

} // namespace duckdb_fastpforlib
