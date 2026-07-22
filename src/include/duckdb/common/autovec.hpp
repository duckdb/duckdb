//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/autovec.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

// Autovec fastpath (bitunpack-shuffle, comparison-to-bitmap, +,-,*): compiled on autovec-capable toolchains/targets.
#if !defined(DUCKDB_SMALLER_BINARY) && (defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 12)) &&                \
    (defined(__x86_64__) || defined(__aarch64__))
#define DUCKDB_AUTOVEC 1
#else
#define DUCKDB_AUTOVEC 0
#endif

// Widen the hot loops to the platform vector ISA without the whole TU needing -march.
#if DUCKDB_AUTOVEC && defined(__x86_64__)
#define DUCKDB_AUTOVEC_TARGET __attribute__((target("avx2")))
#else
#define DUCKDB_AUTOVEC_TARGET
#endif

// MSVC cannot parse __restrict on template-dependent pointer types
#if defined(_MSC_VER)
#define DUCKDB_BITPACKING_RESTRICT
#else
#define DUCKDB_BITPACKING_RESTRICT __restrict
#endif

namespace duckdb {
// True only where an autovec fastpath exists: x86 checks the widened ISA at runtime; aarch64 has it as
// baseline; other targets have none.
inline bool CpuBenefitsFromAutoVec() {
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
	// DUCKDB_DISABLE_AVX2 is the debugging kill-switch for every widened-ISA fastpath
	static const bool enabled = __builtin_cpu_supports("avx2") && !getenv("DUCKDB_DISABLE_AVX2");
	return enabled;
#elif defined(__aarch64__)
	return true;
#else
	return false; // MSVC and exotic compilers: no autovec fastpath (DUCKDB_AUTOVEC is 0 there anyway)
#endif
}

// Gate for the bitmap-selection fastpaths: false on non-autovec toolchains/CPUs, so callers use the scalar path.
inline bool BitmapSelectionEnabled() {
#if DUCKDB_AUTOVEC
	return CpuBenefitsFromAutoVec();
#else
	return false;
#endif
}

// Dense-vs-gather decision: evaluating all `span` values densely costs ~span/lanes vector ops (a 32-byte
// vector holds 32/type_width lanes), gathering costs ~1 op per selected row. Dense pays off when the
// lane-adjusted dense cost does not exceed the selected row count.
inline bool DenseAutoVecPaysOff(size_t selected, size_t span, size_t type_width) {
	return selected * (32 / type_width) >= span;
}

} // namespace duckdb

#if DUCKDB_AUTOVEC
namespace duckdb {

typedef uint8_t duckdb_av_u8x32 __attribute__((vector_size(32)));
typedef uint16_t duckdb_av_u16x16 __attribute__((vector_size(32)));
typedef uint32_t duckdb_av_u32x8 __attribute__((vector_size(32)));
typedef uint64_t duckdb_av_u64x4 __attribute__((vector_size(32)));

//! OR-reduce of 8 weighted lanes: a store + scalar ORs beats a cross-lane shuffle tree.
DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMaskReduce(duckdb_av_u32x8 v) {
	uint32_t buf[8];
	std::memcpy(buf, &v, 32);
	return (buf[0] | buf[1]) | (buf[2] | buf[3]) | ((buf[4] | buf[5]) | (buf[6] | buf[7]));
}

//! MoveMask: pack each lane's top bit into a mask word (lane 0 = bit 0). Inputs are full-lane
//! 0/-1 comparison results. The default weights each lane and OR-reduces; where the widened-ISA
//! define is set, the single-instruction compiler builtins do it instead.

//! 32 x 8-bit lanes -> 32-bit mask
DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMask(duckdb_av_u8x32 v) {
#if defined(__x86_64__)
	typedef char duckdb_av_c8x32 __attribute__((vector_size(32)));
	return static_cast<uint32_t>(__builtin_ia32_pmovmskb256((duckdb_av_c8x32)v));
#else
	// nibble weights per u32 lane, fold, then position each lane's nibble
	const duckdb_av_u8x32 wb = {1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8,
	                            1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8};
	duckdb_av_u32x8 m = (duckdb_av_u32x8)(v & wb);
	m = m | (m >> 16);
	m = (m | (m >> 8)) & 0xF;
	m <<= duckdb_av_u32x8 {0, 4, 8, 12, 16, 20, 24, 28};
	return MoveMaskReduce(m);
#endif
}

//! 2 x 16 x 16-bit lanes -> 32-bit mask (both halves at once: x86 packs then byte-movemasks)
DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMask(duckdb_av_u16x16 lo, duckdb_av_u16x16 hi) {
#if defined(__x86_64__)
	typedef short duckdb_av_s16x16 __attribute__((vector_size(32)));
	typedef char duckdb_av_c8x32 __attribute__((vector_size(32)));
	// the pack interleaves 128-bit lanes; the u64 shuffle restores logical order
	auto packed = (duckdb_av_u64x4)__builtin_ia32_packsswb256((duckdb_av_s16x16)lo, (duckdb_av_s16x16)hi);
	auto fixed = (duckdb_av_c8x32)__builtin_shufflevector(packed, packed, 0, 2, 1, 3);
	return static_cast<uint32_t>(__builtin_ia32_pmovmskb256(fixed));
#else
	// 16-bit lane weights; the pair-fold leaves each u32 lane's bits at their absolute positions
	const duckdb_av_u16x16 w = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768};
	duckdb_av_u32x8 a0 = (duckdb_av_u32x8)(lo & w);
	a0 |= a0 >> 16;
	duckdb_av_u32x8 a1 = (duckdb_av_u32x8)(hi & w);
	a1 |= a1 >> 16;
	return MoveMaskReduce((a0 & 0xFFFF) | (a1 << 16));
#endif
}

//! 8 x 32-bit lanes -> 8-bit mask
DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMask(duckdb_av_u32x8 v) {
#if defined(__x86_64__)
	typedef float duckdb_av_f32x8 __attribute__((vector_size(32)));
	return static_cast<uint32_t>(__builtin_ia32_movmskps256((duckdb_av_f32x8)v));
#else
	return MoveMaskReduce(v & duckdb_av_u32x8 {1, 2, 4, 8, 16, 32, 64, 128});
#endif
}

//! 4 x 64-bit lanes -> 4-bit mask
DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMask(duckdb_av_u64x4 v) {
#if defined(__x86_64__)
	typedef double duckdb_av_f64x4 __attribute__((vector_size(32)));
	return static_cast<uint32_t>(__builtin_ia32_movmskpd256((duckdb_av_f64x4)v));
#else
	auto m = v & duckdb_av_u64x4 {1, 2, 4, 8};
	m |= __builtin_shufflevector(m, m, 2, 3, 2, 3);
	m |= __builtin_shufflevector(m, m, 1, 1, 1, 1);
	return static_cast<uint32_t>(m[0]);
#endif
}

} // namespace duckdb
#endif

#if DUCKDB_AUTOVEC
namespace duckdb_bitpacking {
namespace internal {

// Shuffle bitunpack fastpath: 8 values/iteration. Byte-permute each value's window into a 32-bit lane (one builtin
// gather), per-lane shift and mask, then narrow to the output width. The scalar tail is left to the caller.
typedef uint8_t duckdb_bp_u8x8 __attribute__((vector_size(8)));
typedef uint8_t duckdb_bp_u8x16 __attribute__((vector_size(16)));
typedef uint8_t duckdb_bp_u8x32 __attribute__((vector_size(32)));
typedef uint16_t duckdb_bp_u16x8 __attribute__((vector_size(16)));
typedef uint32_t duckdb_bp_u32x4 __attribute__((vector_size(16)));
typedef uint32_t duckdb_bp_u32x8 __attribute__((vector_size(32)));
typedef uint64_t duckdb_bp_u64x2 __attribute__((vector_size(16)));
typedef uint64_t duckdb_bp_u64x4 __attribute__((vector_size(32)));

// Gather 8 values (0..7) from one 16-byte window into a u32x8; each value's window byte is value*WIDTH/8 (LE).
template <uint32_t WIDTH, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline duckdb_bp_u32x8 ShuffleGather8(duckdb_bp_u8x16 w, std::index_sequence<I...>) {
	return (duckdb_bp_u32x8)__builtin_shufflevector(w, w, static_cast<int>((I / 4) * WIDTH / 8 + I % 4)...);
}
template <uint32_t WIDTH, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline duckdb_bp_u32x8 ShuffleShift8(std::index_sequence<K...>) {
	return duckdb_bp_u32x8 {static_cast<uint32_t>((K * WIDTH) % 8)...};
}
// Gather 4 values [BASE..BASE+3] into a u32x4 from a window whose first byte is packed byte WBYTE (uint32 output,
// where 8 values span more than one 16-byte window).
template <uint32_t WIDTH, uint32_t BASE, uint32_t WBYTE, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline duckdb_bp_u32x4 ShuffleGather4(duckdb_bp_u8x16 w, std::index_sequence<I...>) {
	return (duckdb_bp_u32x4)__builtin_shufflevector(w, w,
	                                                static_cast<int>(((BASE + I / 4) * WIDTH) / 8 - WBYTE + I % 4)...);
}
template <uint32_t WIDTH, uint32_t BASE, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline duckdb_bp_u32x4 ShuffleShift4(std::index_sequence<K...>) {
	return duckdb_bp_u32x4 {static_cast<uint32_t>(((BASE + K) * WIDTH) % 8)...};
}

// Gather the two bytes holding each of 8 values into u16 lanes (a value at bit offset o spans bytes [o/8, o/8+1]
// whenever o%8 + WIDTH <= 16, i.e. the whole value fits its byte pair).
template <uint32_t WIDTH, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline duckdb_bp_u16x8 ShuffleGatherPair(duckdb_bp_u8x16 w, std::index_sequence<I...>) {
	return (duckdb_bp_u16x8)__builtin_shufflevector(w, w, static_cast<int>(((I / 2) * WIDTH) / 8 + I % 2)...);
}
// Per-lane multiplier lifting value k (at bit (k*WIDTH)%8) to the common shift S.
template <uint32_t WIDTH, uint32_t S, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline duckdb_bp_u16x8 ShuffleMulPair(std::index_sequence<K...>) {
	return duckdb_bp_u16x8 {static_cast<uint16_t>(1u << (S - (K * WIDTH) % 8))...};
}
// The multiply-shift path needs bit-offset + WIDTH <= 16 for every value in a group of 8.
template <uint32_t WIDTH>
static constexpr bool UseMulShift16() {
	return WIDTH + (WIDTH % 8 == 0 ? 0 : WIDTH % 4 == 0 ? 4 : WIDTH % 2 == 0 ? 6 : 7) <= 16;
}

// Unpack values [BASE, BASE+1] into u64 lanes from the 16-byte window at their first packed byte (widths 27..57,
// where a value spans up to 8 bytes so only 2 fit a window).
template <uint32_t WIDTH, uint32_t BASE, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline duckdb_bp_u64x2 ShuffleGather2(duckdb_bp_u8x16 w, std::index_sequence<I...>) {
	return (duckdb_bp_u64x2)__builtin_shufflevector(
	    w, w, static_cast<int>(((BASE + I / 8) * WIDTH) / 8 - (BASE * WIDTH) / 8 + I % 8)...);
}
template <uint32_t WIDTH, uint32_t BASE>
DUCKDB_AUTOVEC_TARGET static inline void ShuffleUnpack2(const uint8_t *DUCKDB_BITPACKING_RESTRICT base,
                                                        uint64_t *DUCKDB_BITPACKING_RESTRICT out, duckdb_bp_u64x2 mask,
                                                        duckdb_bp_u64x2 frame) {
	duckdb_bp_u8x16 w;
	std::memcpy(&w, base + (BASE * WIDTH) / 8, 16);
	duckdb_bp_u64x2 v = ((ShuffleGather2<WIDTH, BASE>(w, std::make_index_sequence<16> {}) >>
	                      duckdb_bp_u64x2 {(BASE * WIDTH) % 8, ((BASE + 1) * WIDTH) % 8}) &
	                     mask) +
	                    frame;
	std::memcpy(out + BASE, &v, 16);
}

// Unpack 8 values from the window at base, adding `frame` (the frame-of-reference; 0 when unused).
template <uint32_t WIDTH, class OUT_T>
DUCKDB_AUTOVEC_TARGET static inline void ShuffleUnpackIter(const uint8_t *DUCKDB_BITPACKING_RESTRICT base,
                                                           OUT_T *DUCKDB_BITPACKING_RESTRICT out, OUT_T frame) {
	if constexpr (sizeof(OUT_T) <= 2 && UseMulShift16<WIDTH>()) {
		// narrow out: byte-pair gather into u16 lanes, per-lane multiply to a common shift, mask, narrow
		constexpr uint32_t S = 16 - WIDTH;
		const duckdb_bp_u16x8 mask = duckdb_bp_u16x8 {} + static_cast<uint16_t>((1u << WIDTH) - 1);
		duckdb_bp_u8x16 w;
		std::memcpy(&w, base, 16);
		duckdb_bp_u16x8 v = ((ShuffleGatherPair<WIDTH>(w, std::make_index_sequence<16> {}) *
		                      ShuffleMulPair<WIDTH, S>(std::make_index_sequence<8> {})) >>
		                     S) &
		                    mask;
		v += static_cast<uint16_t>(frame);
		if constexpr (sizeof(OUT_T) == 2) {
			std::memcpy(out, &v, 16);
		} else {
			duckdb_bp_u8x8 o = __builtin_shufflevector((duckdb_bp_u8x16)v, (duckdb_bp_u8x16)v, 0, 2, 4, 6, 8, 10, 12, 14);
			std::memcpy(out, &o, 8);
		}
	} else if constexpr (sizeof(OUT_T) <= 2 || (sizeof(OUT_T) == 8 && WIDTH <= 13)) {
		// 8 values fit one 16-byte window: gather to u32x8, shift, mask, then convert to the output width.
		duckdb_bp_u8x16 w;
		std::memcpy(&w, base, 16);
		const duckdb_bp_u32x8 mask = duckdb_bp_u32x8 {} + static_cast<uint32_t>((uint64_t(1) << WIDTH) - 1);
		duckdb_bp_u32x8 v = (ShuffleGather8<WIDTH>(w, std::make_index_sequence<32> {}) >>
		                     ShuffleShift8<WIDTH>(std::make_index_sequence<8> {})) &
		                    mask;
		if constexpr (sizeof(OUT_T) == 8) {
			// uint64 out: widen each u32x4 half, then add the frame (it can exceed 32 bits)
			const duckdb_bp_u64x4 fr = duckdb_bp_u64x4 {} + frame;
			duckdb_bp_u32x4 l = __builtin_shufflevector(v, v, 0, 1, 2, 3);
			duckdb_bp_u32x4 h = __builtin_shufflevector(v, v, 4, 5, 6, 7);
			duckdb_bp_u64x4 wlo = __builtin_convertvector(l, duckdb_bp_u64x4) + fr;
			duckdb_bp_u64x4 whi = __builtin_convertvector(h, duckdb_bp_u64x4) + fr;
			std::memcpy(out + 0, &wlo, 32);
			std::memcpy(out + 4, &whi, 32);
		} else if constexpr (sizeof(OUT_T) == 2) {
			duckdb_bp_u16x8 o = __builtin_convertvector(v + static_cast<uint32_t>(frame), duckdb_bp_u16x8);
			std::memcpy(out, &o, 16);
		} else {
			// gcc scalarizes convertvector to u8; picking the low byte of each u32 lane keeps it a vector shuffle.
			v += static_cast<uint32_t>(frame);
			duckdb_bp_u8x8 o =
			    __builtin_shufflevector((duckdb_bp_u8x32)v, (duckdb_bp_u8x32)v, 0, 4, 8, 12, 16, 20, 24, 28);
			std::memcpy(out, &o, 8);
		}
	} else if constexpr (WIDTH <= 26) {
		// uint32/uint64: 8 values span more than 16 bytes, so gather 4+4 from two windows into u32 lanes.
		constexpr uint32_t wb1 = (4 * WIDTH) / 8;
		const auto seq16 = std::make_index_sequence<16> {};
		const duckdb_bp_u32x4 mask = duckdb_bp_u32x4 {} + static_cast<uint32_t>((uint64_t(1) << WIDTH) - 1);
		duckdb_bp_u8x16 w0, w1;
		std::memcpy(&w0, base, 16);
		std::memcpy(&w1, base + wb1, 16);
		duckdb_bp_u32x4 lo =
		    (ShuffleGather4<WIDTH, 0, 0>(w0, seq16) >> ShuffleShift4<WIDTH, 0>(std::make_index_sequence<4> {})) & mask;
		duckdb_bp_u32x4 hi =
		    (ShuffleGather4<WIDTH, 4, wb1>(w1, seq16) >> ShuffleShift4<WIDTH, 4>(std::make_index_sequence<4> {})) &
		    mask;
		if constexpr (sizeof(OUT_T) == 4) {
			const duckdb_bp_u32x4 fr = duckdb_bp_u32x4 {} + static_cast<uint32_t>(frame);
			lo += fr;
			hi += fr;
			std::memcpy(out + 0, &lo, 16);
			std::memcpy(out + 4, &hi, 16);
		} else {
			// uint64 out: widen the 4-byte lanes, then add the frame (it can exceed 32 bits)
			const duckdb_bp_u64x4 fr = duckdb_bp_u64x4 {} + frame;
			duckdb_bp_u64x4 wlo = __builtin_convertvector(lo, duckdb_bp_u64x4) + fr;
			duckdb_bp_u64x4 whi = __builtin_convertvector(hi, duckdb_bp_u64x4) + fr;
			std::memcpy(out + 0, &wlo, 32);
			std::memcpy(out + 4, &whi, 32);
		}
	} else {
		// uint64 widths 27..57: four windows of 2 values each, gathered straight into u64 lanes.
		const duckdb_bp_u64x2 mask = duckdb_bp_u64x2 {} + ((uint64_t(1) << WIDTH) - 1);
		const duckdb_bp_u64x2 fr = duckdb_bp_u64x2 {} + frame;
		uint64_t *DUCKDB_BITPACKING_RESTRICT out64 = reinterpret_cast<uint64_t *>(out);
		ShuffleUnpack2<WIDTH, 0>(base, out64, mask, fr);
		ShuffleUnpack2<WIDTH, 2>(base, out64, mask, fr);
		ShuffleUnpack2<WIDTH, 4>(base, out64, mask, fr);
		ShuffleUnpack2<WIDTH, 6>(base, out64, mask, fr);
	}
}

// Widths the shuffle path handles: a value must fit a 4-byte gather lane (<= 26) for outputs up to uint32, or an
// 8-byte lane offset within a 16-byte window (<= 57) for uint64. WIDTH == output width stays scalar (a memcpy).
template <uint32_t WIDTH, class OUT_T>
static constexpr bool UseShuffleUnpack() {
	return WIDTH > 0 && WIDTH < 8 * sizeof(OUT_T) && sizeof(OUT_T) <= 8 && WIDTH <= (sizeof(OUT_T) == 8 ? 57 : 26);
}

// Unpack the leading vectorizable groups (32 values each) and return their count; the caller unpacks the rest
// scalar. The windowed loads read a few bytes past an 8-value chunk, so the trailing `reserve` groups are excluded.
template <uint32_t WIDTH, class OUT_T>
DUCKDB_AUTOVEC_TARGET static inline std::size_t ShuffleUnpack(const uint32_t *DUCKDB_BITPACKING_RESTRICT in,
                                                              OUT_T *DUCKDB_BITPACKING_RESTRICT out, std::size_t groups,
                                                              OUT_T frame = 0) {
	constexpr std::size_t width = WIDTH;
	constexpr std::size_t window = (sizeof(OUT_T) == 8 && WIDTH > 26) ? (6 * width) / 8 : (4 * width) / 8;
	constexpr std::size_t reserve = (window + 16 + 4 * width - 1) / (4 * width);
	const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
	const uint8_t *DUCKDB_BITPACKING_RESTRICT base = reinterpret_cast<const uint8_t *>(in);
	for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values/iteration, 4 iterations/group
		ShuffleUnpackIter<WIDTH, OUT_T>(base + s * WIDTH, out + s * 8, frame);
	}
	return shuffle_groups;
}

} // namespace internal
} // namespace duckdb_bitpacking
#endif
