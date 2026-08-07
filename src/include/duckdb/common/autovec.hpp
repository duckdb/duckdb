//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/autovec.hpp
//
// Auto-vectorization helpers for bit-unpacking and bitmap materialization.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/smaller_binary.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

#if (defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 12)) && (defined(__x86_64__) || defined(__aarch64__)) &&  \
    !DUCKDB_SMALLER_BINARY(autovec) // clang/gcc on x86/aarch64
#define DUCKDB_AUTOVEC 1
#else
#define DUCKDB_AUTOVEC 0
#endif

#if DUCKDB_AUTOVEC && defined(__x86_64__)
#define DUCKDB_AUTOVEC_TARGET __attribute__((target("avx2"))) // widened x86 kernels
#else
#define DUCKDB_AUTOVEC_TARGET
#endif

#if defined(_MSC_VER)
#define DUCKDB_BITPACKING_RESTRICT
#else
#define DUCKDB_BITPACKING_RESTRICT __restrict // MSVC cannot parse dependent __restrict pointers
#endif

// Pin the unroll factor of hot dense loops: ops without a vector instruction (e.g. 64-bit multiply
// on NEON) otherwise compile to a minimal scalar loop whose throughput varies with code placement.
#if defined(__clang__)
#define DUCKDB_UNROLL_LOOP _Pragma("clang loop unroll_count(4)")
#elif defined(__GNUC__)
#define DUCKDB_UNROLL_LOOP _Pragma("GCC unroll 8")
#else
#define DUCKDB_UNROLL_LOOP
#endif

namespace duckdb {

inline bool CpuBenefitsFromAutoVec() {
#if !DUCKDB_AUTOVEC
	return false; // not compiled in
#elif defined(__aarch64__)
	return true; // NEON is always available
#else
	static const bool enabled = __builtin_cpu_supports("avx2") && !getenv("DUCKDB_DISABLE_AVX2"); // cached
	return enabled;
#endif
}

inline bool DenseAutoVecPaysOff(size_t selected, size_t span, size_t type_width) {
	return type_width && (selected * (32 / type_width) >= span); // 0-width (nested) payloads => no autovec
}

#if DUCKDB_AUTOVEC

typedef uint8_t duckdb_av_u8x16 __attribute__((vector_size(16)));
typedef uint8_t duckdb_av_u8x32 __attribute__((vector_size(32)));
typedef uint16_t duckdb_av_u16x8 __attribute__((vector_size(16)));
typedef uint16_t duckdb_av_u16x16 __attribute__((vector_size(32)));
typedef uint32_t duckdb_av_u32x4 __attribute__((vector_size(16)));
typedef uint32_t duckdb_av_u32x8 __attribute__((vector_size(32)));
typedef uint64_t duckdb_av_u64x2 __attribute__((vector_size(16)));
typedef uint64_t duckdb_av_u64x4 __attribute__((vector_size(32)));

// Shuffle bit-unpack primitives.
template <class VEC, uint32_t WIDTH, uint32_t BASE, uint32_t WBYTE, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline VEC ShuffleGatherImpl(duckdb_av_u8x16 w, std::index_sequence<I...>) {
	constexpr std::size_t LB = sizeof(VEC {}[0]);
	return (VEC)__builtin_shufflevector(w, w, static_cast<int>(((BASE + I / LB) * WIDTH) / 8 - WBYTE + I % LB)...);
}
template <class VEC, uint32_t WIDTH, uint32_t BASE = 0, uint32_t WBYTE = 0>
DUCKDB_AUTOVEC_TARGET static inline VEC ShuffleGather(duckdb_av_u8x16 w) {
	return ShuffleGatherImpl<VEC, WIDTH, BASE, WBYTE>(w, std::make_index_sequence<sizeof(VEC)> {});
}
template <class VEC, uint32_t WIDTH, uint32_t BASE, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline VEC ShuffleShiftsImpl(std::index_sequence<K...>) {
	return VEC {static_cast<decltype(VEC {}[0])>(((BASE + K) * WIDTH) % 8)...};
}
template <class VEC, uint32_t WIDTH, uint32_t BASE = 0>
DUCKDB_AUTOVEC_TARGET static inline VEC ShuffleShifts() {
	return ShuffleShiftsImpl<VEC, WIDTH, BASE>(std::make_index_sequence<sizeof(VEC) / sizeof(VEC {}[0])> {});
}

template <uint32_t WIDTH>
static constexpr bool UseMulShift16() { // narrow outputs decode well in 16-bit lanes
	return WIDTH > 0 && WIDTH + (WIDTH % 8 == 0 ? 0 : WIDTH % 4 == 0 ? 4 : WIDTH % 2 == 0 ? 6 : 7) <= 16;
}
template <uint32_t WIDTH, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline duckdb_av_u16x16 ShuffleGatherPair(duckdb_av_u8x16 lo, duckdb_av_u8x16 hi,
                                                                       std::index_sequence<I...>) {
	return (duckdb_av_u16x16)__builtin_shufflevector(
	    lo, hi, static_cast<int>((I / 16) * 16 + (((I / 2) % 8) * WIDTH) / 8 + I % 2)...);
}
template <uint32_t WIDTH, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline duckdb_av_u16x16 MulShift16(std::index_sequence<K...>) {
	return duckdb_av_u16x16 {static_cast<uint16_t>(1u << ((16 - WIDTH) - ((K % 8) * WIDTH) % 8))...};
}
template <uint32_t WIDTH>
DUCKDB_AUTOVEC_TARGET static inline duckdb_av_u16x16 Decode16(const uint8_t *DUCKDB_BITPACKING_RESTRICT base) {
	if constexpr (WIDTH == 8) {
		duckdb_av_u8x16 b;
		std::memcpy(&b, base, 16);
		return __builtin_convertvector(b, duckdb_av_u16x16);
	} else {
		duckdb_av_u8x16 lo, hi;
		std::memcpy(&lo, base, 16);
		std::memcpy(&hi, base + WIDTH, 16);
		return (ShuffleGatherPair<WIDTH>(lo, hi, std::make_index_sequence<32> {}) * // multiply shifts bits up
		        MulShift16<WIDTH>(std::make_index_sequence<16> {})) >>
		       (16 - WIDTH);
	}
}
template <uint32_t WIDTH, class OUT_T>
DUCKDB_AUTOVEC_TARGET static inline void ShuffleUnpackIter32(const uint8_t *DUCKDB_BITPACKING_RESTRICT base,
                                                             OUT_T *DUCKDB_BITPACKING_RESTRICT out, OUT_T frame) {
	duckdb_av_u16x16 v0 = Decode16<WIDTH>(base) + static_cast<uint16_t>(frame); // first 16 values
	duckdb_av_u16x16 v1 = Decode16<WIDTH>(base + 2 * WIDTH) + static_cast<uint16_t>(frame);
	if constexpr (sizeof(OUT_T) == 2) {
		std::memcpy(out, &v0, 32);
		std::memcpy(out + 16, &v1, 32);
	} else {
		duckdb_av_u8x32 o =
		    __builtin_shufflevector((duckdb_av_u8x32)v0, (duckdb_av_u8x32)v1, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22,
		                            24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62);
		std::memcpy(out, &o, 32);
	}
}
template <uint32_t WIDTH, uint32_t BASE>
DUCKDB_AUTOVEC_TARGET static inline void ShuffleUnpack2(const uint8_t *DUCKDB_BITPACKING_RESTRICT base,
                                                        uint64_t *DUCKDB_BITPACKING_RESTRICT out, duckdb_av_u64x2 mask,
                                                        duckdb_av_u64x2 frame) {
	duckdb_av_u8x16 w;
	std::memcpy(&w, base + (BASE * WIDTH) / 8, 16); // two u64 values per window
	duckdb_av_u64x2 v = ((ShuffleGather<duckdb_av_u64x2, WIDTH, BASE, (BASE * WIDTH) / 8>(w) >>
	                      ShuffleShifts<duckdb_av_u64x2, WIDTH, BASE>()) &
	                     mask) +
	                    frame;
	std::memcpy(out + BASE, &v, 16);
}

template <uint32_t WIDTH, class OUT_T>
DUCKDB_AUTOVEC_TARGET static inline void ShuffleUnpackIter(const uint8_t *DUCKDB_BITPACKING_RESTRICT base,
                                                           OUT_T *DUCKDB_BITPACKING_RESTRICT out, OUT_T frame) {
	if constexpr (sizeof(OUT_T) == 2 || (sizeof(OUT_T) == 8 && WIDTH <= 13)) {
		duckdb_av_u8x16 w;
		std::memcpy(&w, base, 16);
		const duckdb_av_u32x8 mask = duckdb_av_u32x8 {} + static_cast<uint32_t>((uint64_t(1) << WIDTH) - 1);
		duckdb_av_u32x8 v =
		    (ShuffleGather<duckdb_av_u32x8, WIDTH>(w) >> ShuffleShifts<duckdb_av_u32x8, WIDTH>()) & mask;
		if constexpr (sizeof(OUT_T) == 8) {
			const duckdb_av_u64x4 fr = duckdb_av_u64x4 {} + frame;
			duckdb_av_u32x4 l = __builtin_shufflevector(v, v, 0, 1, 2, 3);
			duckdb_av_u32x4 h = __builtin_shufflevector(v, v, 4, 5, 6, 7);
			duckdb_av_u64x4 wlo = __builtin_convertvector(l, duckdb_av_u64x4) + fr;
			duckdb_av_u64x4 whi = __builtin_convertvector(h, duckdb_av_u64x4) + fr;
			std::memcpy(out + 0, &wlo, 32);
			std::memcpy(out + 4, &whi, 32);
		} else {
			duckdb_av_u16x8 o = __builtin_convertvector(v + static_cast<uint32_t>(frame), duckdb_av_u16x8);
			std::memcpy(out, &o, 16);
		}
	} else if constexpr (WIDTH <= 26) {
		constexpr uint32_t wb1 = (4 * WIDTH) / 8;
		const duckdb_av_u32x4 mask = duckdb_av_u32x4 {} + static_cast<uint32_t>((uint64_t(1) << WIDTH) - 1);
		duckdb_av_u8x16 w0, w1;
		std::memcpy(&w0, base, 16);
		std::memcpy(&w1, base + wb1, 16);
		duckdb_av_u32x4 lo =
		    (ShuffleGather<duckdb_av_u32x4, WIDTH>(w0) >> ShuffleShifts<duckdb_av_u32x4, WIDTH>()) & mask;
		duckdb_av_u32x4 hi =
		    (ShuffleGather<duckdb_av_u32x4, WIDTH, 4, wb1>(w1) >> ShuffleShifts<duckdb_av_u32x4, WIDTH, 4>()) & mask;
		if constexpr (sizeof(OUT_T) == 4) {
			const duckdb_av_u32x4 fr = duckdb_av_u32x4 {} + static_cast<uint32_t>(frame);
			lo += fr;
			hi += fr;
			std::memcpy(out + 0, &lo, 16);
			std::memcpy(out + 4, &hi, 16);
		} else {
			const duckdb_av_u64x4 fr = duckdb_av_u64x4 {} + frame;
			duckdb_av_u64x4 wlo = __builtin_convertvector(lo, duckdb_av_u64x4) + fr;
			duckdb_av_u64x4 whi = __builtin_convertvector(hi, duckdb_av_u64x4) + fr;
			std::memcpy(out + 0, &wlo, 32);
			std::memcpy(out + 4, &whi, 32);
		}
	} else {
		const duckdb_av_u64x2 mask = duckdb_av_u64x2 {} + ((uint64_t(1) << WIDTH) - 1);
		const duckdb_av_u64x2 fr = duckdb_av_u64x2 {} + frame;
		uint64_t *DUCKDB_BITPACKING_RESTRICT out64 = reinterpret_cast<uint64_t *>(out);
		ShuffleUnpack2<WIDTH, 0>(base, out64, mask, fr);
		ShuffleUnpack2<WIDTH, 2>(base, out64, mask, fr);
		ShuffleUnpack2<WIDTH, 4>(base, out64, mask, fr);
		ShuffleUnpack2<WIDTH, 6>(base, out64, mask, fr);
	}
}

template <uint32_t WIDTH, class OUT_T>
static constexpr bool UseShuffleUnpack() { // exclude widths that overrun the shuffle window
	return WIDTH > 0 && WIDTH < 8 * sizeof(OUT_T) && sizeof(OUT_T) <= 8 && WIDTH <= (sizeof(OUT_T) == 8 ? 57 : 26);
}

template <uint32_t WIDTH, class OUT_T>
DUCKDB_AUTOVEC_TARGET static inline std::size_t ShuffleUnpack(const uint32_t *DUCKDB_BITPACKING_RESTRICT in,
                                                              OUT_T *DUCKDB_BITPACKING_RESTRICT out, std::size_t groups,
                                                              OUT_T frame = 0) {
	constexpr std::size_t width = WIDTH;
	const uint8_t *DUCKDB_BITPACKING_RESTRICT base = reinterpret_cast<const uint8_t *>(in);
	if constexpr (sizeof(OUT_T) <= 2 && UseMulShift16<WIDTH>()) {
		constexpr std::size_t reserve = (3 * width + 16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups; s++) {
			ShuffleUnpackIter32<WIDTH, OUT_T>(base + s * 4 * WIDTH, out + s * 32, frame);
		}
		return shuffle_groups;
	} else {
		constexpr std::size_t window = (sizeof(OUT_T) == 8 && WIDTH > 26) ? (6 * width) / 8 : (4 * width) / 8;
		constexpr std::size_t reserve = (window + 16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) {
			ShuffleUnpackIter<WIDTH, OUT_T>(base + s * WIDTH, out + s * 8, frame);
		}
		return shuffle_groups;
	}
}

// MoveMask packs lane comparison bits into a bitmap word.
DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMaskReduce(duckdb_av_u32x8 v) {
	uint32_t buf[8];
	std::memcpy(buf, &v, 32);
	return (buf[0] | buf[1]) | (buf[2] | buf[3]) | ((buf[4] | buf[5]) | (buf[6] | buf[7]));
}

template <class V, std::size_t... K>
DUCKDB_AUTOVEC_TARGET inline V MoveMaskWeights(std::index_sequence<K...>) {
	constexpr std::size_t CAP = sizeof(V {}[0]) == 1 ? 4 : 8 * sizeof(V {}[0]);
	return V {static_cast<decltype(V {}[0])>(1ull << (K % CAP))...};
}

template <class V>
DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMask(V v) {
	constexpr std::size_t LANE = sizeof(V {}[0]);
	static_assert(LANE == 1 || LANE == 4 || LANE == 8, "16-bit lanes use the two-vector MoveMask");
#if defined(__x86_64__)
	if constexpr (LANE == 1) {
		typedef char duckdb_av_c8x32 __attribute__((vector_size(32)));
		return static_cast<uint32_t>(__builtin_ia32_pmovmskb256((duckdb_av_c8x32)v));
	} else if constexpr (LANE == 4) {
		typedef float duckdb_av_f32x8 __attribute__((vector_size(32)));
		return static_cast<uint32_t>(__builtin_ia32_movmskps256((duckdb_av_f32x8)v));
	} else {
		typedef double duckdb_av_f64x4 __attribute__((vector_size(32)));
		return static_cast<uint32_t>(__builtin_ia32_movmskpd256((duckdb_av_f64x4)v));
	}
#else
	const V w = MoveMaskWeights<V>(std::make_index_sequence<sizeof(V) / LANE> {}); // portable lane weights
	if constexpr (LANE == 1) {
		duckdb_av_u32x8 m = (duckdb_av_u32x8)(v & w);
		m = m | (m >> 16);
		m = (m | (m >> 8)) & 0xF;
		m <<= duckdb_av_u32x8 {0, 4, 8, 12, 16, 20, 24, 28};
		return MoveMaskReduce(m);
	} else if constexpr (LANE == 4) {
		return MoveMaskReduce(v & w);
	} else {
		auto m = v & w;
		m |= __builtin_shufflevector(m, m, 2, 3, 2, 3);
		m |= __builtin_shufflevector(m, m, 1, 1, 1, 1);
		return static_cast<uint32_t>(m[0]);
	}
#endif
}

DUCKDB_AUTOVEC_TARGET inline uint32_t MoveMask(duckdb_av_u16x16 lo, duckdb_av_u16x16 hi) {
#if defined(__x86_64__)
	typedef short duckdb_av_s16x16 __attribute__((vector_size(32)));
	typedef char duckdb_av_c8x32 __attribute__((vector_size(32)));
	// 16-bit lanes to bytes
	auto packed = (duckdb_av_u64x4)__builtin_ia32_packsswb256((duckdb_av_s16x16)lo, (duckdb_av_s16x16)hi);
	auto fixed = (duckdb_av_c8x32)__builtin_shufflevector(packed, packed, 0, 2, 1, 3);
	return static_cast<uint32_t>(__builtin_ia32_pmovmskb256(fixed));
#else
	const duckdb_av_u16x16 w = MoveMaskWeights<duckdb_av_u16x16>(std::make_index_sequence<16> {});
	duckdb_av_u32x8 a0 = (duckdb_av_u32x8)(lo & w);
	duckdb_av_u32x8 a1 = (duckdb_av_u32x8)(hi & w);
	a0 |= a0 >> 16;
	a1 |= a1 >> 16;
	return MoveMaskReduce((a0 & 0xFFFF) | (a1 << 16));
#endif
}

#endif // DUCKDB_AUTOVEC

} // namespace duckdb
