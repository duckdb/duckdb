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
	return __builtin_cpu_supports("avx2");
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

} // namespace duckdb

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
	if constexpr (sizeof(OUT_T) <= 2 || (sizeof(OUT_T) == 8 && WIDTH <= 13)) {
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
	constexpr std::size_t window = (sizeof(OUT_T) == 8 && WIDTH > 26) ? (6 * WIDTH) / 8 : (4 * WIDTH) / 8;
	constexpr std::size_t reserve = (window + 16 + 4 * WIDTH - 1) / (4 * WIDTH);
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
