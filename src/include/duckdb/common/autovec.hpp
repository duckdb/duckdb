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
#define DUCKDB_AUTOVEC_MUL16 1 // AVX2 has no per-lane 8/16-bit shifts: high-align 16-bit lanes via multiply
#else
#define DUCKDB_AUTOVEC_TARGET
#define DUCKDB_AUTOVEC_MUL16 0 // NEON shifts every lane width: keep the cheaper shift/combine kernel
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
DUCKDB_AUTOVEC_TARGET static inline VEC LoGatherImpl(duckdb_av_u8x16 w, std::index_sequence<I...>) {
	constexpr std::size_t LB = sizeof(VEC {}[0]);
	return (VEC)__builtin_shufflevector(w, w, static_cast<int>(((BASE + I / LB) * WIDTH) / 8 - WBYTE + I % LB)...);
}
template <class VEC, uint32_t WIDTH, uint32_t BASE = 0, uint32_t WBYTE = 0>
DUCKDB_AUTOVEC_TARGET static inline VEC LoGather(duckdb_av_u8x16 w) {
	return LoGatherImpl<VEC, WIDTH, BASE, WBYTE>(w, std::make_index_sequence<sizeof(VEC)> {});
}
template <class VEC, uint32_t WIDTH, uint32_t BASE, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline VEC LoShiftsImpl(std::index_sequence<K...>) {
	return VEC {static_cast<decltype(VEC {}[0])>(((BASE + K) * WIDTH) % 8)...};
}
template <class VEC, uint32_t WIDTH, uint32_t BASE = 0>
DUCKDB_AUTOVEC_TARGET static inline VEC LoShifts() {
	return LoShiftsImpl<VEC, WIDTH, BASE>(std::make_index_sequence<sizeof(VEC) / sizeof(VEC {}[0])> {});
}

//! Does any lane's value spill past its lane after the in-byte shift?
template <class VEC, uint32_t WIDTH, uint32_t BASE>
static constexpr bool ShuffleLaneCrosses() {
	constexpr uint32_t LANEBITS = 8 * sizeof(VEC {}[0]);
	for (uint32_t i = 0; i < sizeof(VEC) / sizeof(VEC {}[0]); i++) {
		if (((BASE + i) * WIDTH) % 8 + WIDTH > LANEBITS) {
			return true;
		}
	}
	return false;
}

//! Gather of the byte after each lane's gathered window: carries the bits a crossing lane is missing.
template <class VEC, uint32_t WIDTH, uint32_t BASE, uint32_t WBYTE, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline VEC HiGatherImpl(duckdb_av_u8x16 w, std::index_sequence<I...>) {
	constexpr std::size_t LB = sizeof(VEC {}[0]);
	return (VEC)__builtin_shufflevector(w, w, static_cast<int>(((BASE + I / LB) * WIDTH) / 8 - WBYTE + 1 + I % LB)...);
}
template <class VEC, uint32_t WIDTH, uint32_t BASE = 0, uint32_t WBYTE = 0>
DUCKDB_AUTOVEC_TARGET static inline VEC HiGather(duckdb_av_u8x16 w) {
	return HiGatherImpl<VEC, WIDTH, BASE, WBYTE>(w, std::make_index_sequence<sizeof(VEC)> {});
}
template <class VEC, uint32_t WIDTH, uint32_t BASE, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline VEC HiShiftsImpl(std::index_sequence<K...>) {
	return VEC {static_cast<decltype(VEC {}[0])>(8 - ((BASE + K) * WIDTH) % 8)...};
}
template <class VEC, uint32_t WIDTH, uint32_t BASE = 0>
DUCKDB_AUTOVEC_TARGET static inline VEC HiShifts() {
	return HiShiftsImpl<VEC, WIDTH, BASE>(std::make_index_sequence<sizeof(VEC) / sizeof(VEC {}[0])> {});
}

//! Per-lane multiplier 2^(LANEBITS-WIDTH-shift): a multiply is the only per-lane left shift that
//! 16-bit lanes have (no vpsllvw/vpsrlvw on x86), and it lands each field on its lane's high edge.
template <class VEC, uint32_t WIDTH, uint32_t BASE, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline VEC HiAlignMulImpl(std::index_sequence<K...>) {
	constexpr uint32_t LANEBITS = 8 * sizeof(VEC {}[0]);
	return VEC {static_cast<decltype(VEC {}[0])>(1u << (LANEBITS - WIDTH - ((BASE + K) * WIDTH) % 8))...};
}
template <class VEC, uint32_t WIDTH, uint32_t BASE = 0>
DUCKDB_AUTOVEC_TARGET static inline VEC HiAlignMul() {
	return HiAlignMulImpl<VEC, WIDTH, BASE>(std::make_index_sequence<sizeof(VEC) / sizeof(VEC {}[0])> {});
}

//! One decode kernel for every width: gather at the value byte, shift unless byte-aligned,
//! OR in the crossing byte where a lane spills (overlapping bits are identical), mask, add frame.
//! 16-bit lanes instead multiply to the lane's high edge and shift back down by a uniform amount,
//! which zero-fills and so needs no mask; lanes whose field outgrows them decode a lane size up.
template <class VEC, uint32_t WIDTH, uint32_t BASE, uint32_t WBYTE>
DUCKDB_AUTOVEC_TARGET static inline VEC ShuffleDecode(duckdb_av_u8x16 w, VEC frame) {
	constexpr uint32_t LANEBITS = 8 * sizeof(VEC {}[0]);
	VEC v = LoGather<VEC, WIDTH, BASE, WBYTE>(w);
	if constexpr (DUCKDB_AUTOVEC_MUL16 && sizeof(VEC {}[0]) == 2 && WIDTH % 8 != 0) {
		static_assert(!ShuffleLaneCrosses<VEC, WIDTH, BASE>(), "high-align needs the field inside its lane");
		return ((v * HiAlignMul<VEC, WIDTH, BASE>()) >> (LANEBITS - WIDTH)) + frame;
	} else {
		if constexpr (WIDTH % 8 != 0) { // byte-aligned widths are a pure gather
			v >>= LoShifts<VEC, WIDTH, BASE>();
		}
		if constexpr (ShuffleLaneCrosses<VEC, WIDTH, BASE>()) {
			v |= HiGather<VEC, WIDTH, BASE, WBYTE>(w) << HiShifts<VEC, WIDTH, BASE>();
		}
		return (v & (VEC {} + static_cast<decltype(VEC {}[0])>((uint64_t(1) << WIDTH) - 1))) + frame;
	}
}

//! Two-register window for u64 widths 57..63: the pair window exceeds 16 bytes.
template <uint32_t WIDTH, uint32_t BASE, bool HI, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline duckdb_av_u64x2 Gather64x2Impl(duckdb_av_u8x16 w0, duckdb_av_u8x16 w1,
                                                                   std::index_sequence<I...>) {
	constexpr uint32_t WBYTE = (BASE * WIDTH) / 8;
	return (duckdb_av_u64x2)__builtin_shufflevector(
	    w0, w1, static_cast<int>(((BASE + I / 8) * WIDTH) / 8 - WBYTE + (HI ? 1 : 0) + I % 8)...);
}
template <uint32_t WIDTH, uint32_t BASE>
DUCKDB_AUTOVEC_TARGET static inline duckdb_av_u64x2 ShuffleDecode64x2(const uint8_t *DUCKDB_BITPACKING_RESTRICT base,
                                                                      duckdb_av_u64x2 frame) {
	const duckdb_av_u64x2 mask = duckdb_av_u64x2 {} + ((uint64_t(1) << WIDTH) - 1);
	constexpr uint32_t WBYTE = (BASE * WIDTH) / 8;
	duckdb_av_u8x16 w0, w1;
	std::memcpy(&w0, base + WBYTE, 16);
	std::memcpy(&w1, base + WBYTE + 16, 16);
	duckdb_av_u64x2 v = Gather64x2Impl<WIDTH, BASE, false>(w0, w1, std::make_index_sequence<16> {}) >>
	                    LoShifts<duckdb_av_u64x2, WIDTH, BASE>();
	if constexpr (ShuffleLaneCrosses<duckdb_av_u64x2, WIDTH, BASE>()) {
		v |= Gather64x2Impl<WIDTH, BASE, true>(w0, w1, std::make_index_sequence<16> {})
		     << HiShifts<duckdb_av_u64x2, WIDTH, BASE>();
	}
	return (v & mask) + frame;
}

template <uint32_t WIDTH, class OUT_T>
static constexpr bool UseShuffleUnpack() { // u32 at width 31 would need a second window register;
	                                       // past u64 width 60 the two-register gather loses to the scalar loop
	return WIDTH > 0 && WIDTH < 8 * sizeof(OUT_T) && sizeof(OUT_T) <= 8 && !(sizeof(OUT_T) == 4 && WIDTH == 31) &&
	       (sizeof(OUT_T) != 8 || WIDTH <= 60);
}

template <uint32_t WIDTH, class OUT_T>
DUCKDB_AUTOVEC_TARGET static inline std::size_t ShuffleUnpack(const uint32_t *DUCKDB_BITPACKING_RESTRICT in,
                                                              OUT_T *DUCKDB_BITPACKING_RESTRICT out, std::size_t groups,
                                                              OUT_T frame = 0) {
	constexpr std::size_t width = WIDTH;
	const uint8_t *DUCKDB_BITPACKING_RESTRICT base = reinterpret_cast<const uint8_t *>(in);
	// a u16 width whose field spills past its lane cannot be high-aligned: decode it in 32-bit lanes
	constexpr bool wide16 = DUCKDB_AUTOVEC_MUL16 && sizeof(OUT_T) == 2 && ShuffleLaneCrosses<duckdb_av_u16x8, WIDTH, 0>();
	if constexpr (sizeof(OUT_T) == 1 && !DUCKDB_AUTOVEC_MUL16 && !ShuffleLaneCrosses<duckdb_av_u8x16, WIDTH, 0>()) {
		const duckdb_av_u8x16 fr = duckdb_av_u8x16 {} + static_cast<uint8_t>(frame);
		constexpr std::size_t reserve = (16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 2; s++) { // 16 values per step
			duckdb_av_u8x16 w;
			std::memcpy(&w, base + s * 2 * WIDTH, 16);
			duckdb_av_u8x16 v = ShuffleDecode<duckdb_av_u8x16, WIDTH, 0, 0>(w, fr);
			std::memcpy(out + s * 16, &v, 16);
		}
		return shuffle_groups;
	} else if constexpr (sizeof(OUT_T) == 1) { // 8-bit lanes have no multiply: decode in 16-bit, narrow at the end
		const duckdb_av_u16x8 fr = duckdb_av_u16x8 {} + static_cast<uint16_t>(frame);
		constexpr std::size_t reserve = (width + 16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 2; s++) { // 16 values per step
			duckdb_av_u8x16 w0, w1;
			std::memcpy(&w0, base + s * 2 * WIDTH, 16);
			std::memcpy(&w1, base + s * 2 * WIDTH + WIDTH, 16);
			duckdb_av_u16x8 a = ShuffleDecode<duckdb_av_u16x8, WIDTH, 0, 0>(w0, fr);
			duckdb_av_u16x8 b = ShuffleDecode<duckdb_av_u16x8, WIDTH, 0, 0>(w1, fr);
			duckdb_av_u8x16 o = __builtin_shufflevector((duckdb_av_u8x16)a, (duckdb_av_u8x16)b, 0, 2, 4, 6, 8, 10, 12,
			                                            14, 16, 18, 20, 22, 24, 26, 28, 30);
			std::memcpy(out + s * 16, &o, 16);
		}
		return shuffle_groups;
	} else if constexpr (sizeof(OUT_T) == 2 && !wide16) {
		const duckdb_av_u16x8 fr = duckdb_av_u16x8 {} + static_cast<uint16_t>(frame);
		constexpr std::size_t reserve = (16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values per step
			duckdb_av_u8x16 w;
			std::memcpy(&w, base + s * WIDTH, 16);
			duckdb_av_u16x8 v = ShuffleDecode<duckdb_av_u16x8, WIDTH, 0, 0>(w, fr);
			std::memcpy(out + s * 8, &v, 16);
		}
		return shuffle_groups;
	} else if constexpr (sizeof(OUT_T) == 4 || wide16) {
		constexpr uint32_t wb1 = (4 * WIDTH) / 8;
		const duckdb_av_u32x4 fr = duckdb_av_u32x4 {} + static_cast<uint32_t>(frame);
		constexpr std::size_t reserve = (wb1 + 16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values per step
			const uint8_t *DUCKDB_BITPACKING_RESTRICT p = base + s * WIDTH;
			duckdb_av_u8x16 w0, w1;
			std::memcpy(&w0, p, 16);
			std::memcpy(&w1, p + wb1, 16);
			duckdb_av_u32x4 lo = ShuffleDecode<duckdb_av_u32x4, WIDTH, 0, 0>(w0, fr);
			duckdb_av_u32x4 hi = ShuffleDecode<duckdb_av_u32x4, WIDTH, 4, wb1>(w1, fr);
			if constexpr (sizeof(OUT_T) == 2) { // narrow the wide-lane detour back down
				duckdb_av_u16x8 o =
				    __builtin_shufflevector((duckdb_av_u16x8)lo, (duckdb_av_u16x8)hi, 0, 2, 4, 6, 8, 10, 12, 14);
				std::memcpy(out + s * 8, &o, 16);
			} else {
				std::memcpy(out + s * 8 + 0, &lo, 16);
				std::memcpy(out + s * 8 + 4, &hi, 16);
			}
		}
		return shuffle_groups;
	} else if constexpr (WIDTH <= 13) { // one window feeds eight 32-bit lanes; widen last
		const duckdb_av_u32x8 zero {};
		const duckdb_av_u64x4 fr = duckdb_av_u64x4 {} + frame;
		constexpr std::size_t reserve = (16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values per step
			duckdb_av_u8x16 w;
			std::memcpy(&w, base + s * WIDTH, 16);
			duckdb_av_u32x8 v = ShuffleDecode<duckdb_av_u32x8, WIDTH, 0, 0>(w, zero);
			duckdb_av_u32x4 l = __builtin_shufflevector(v, v, 0, 1, 2, 3);
			duckdb_av_u32x4 h = __builtin_shufflevector(v, v, 4, 5, 6, 7);
			duckdb_av_u64x4 wlo = __builtin_convertvector(l, duckdb_av_u64x4) + fr;
			duckdb_av_u64x4 whi = __builtin_convertvector(h, duckdb_av_u64x4) + fr;
			std::memcpy(out + s * 8 + 0, &wlo, 32);
			std::memcpy(out + s * 8 + 4, &whi, 32);
		}
		return shuffle_groups;
	} else if constexpr (WIDTH <= 30) { // decode in 32-bit lanes, widen last
		constexpr uint32_t wb1 = (4 * WIDTH) / 8;
		const duckdb_av_u32x4 zero {};
		const duckdb_av_u64x4 fr = duckdb_av_u64x4 {} + frame;
		constexpr std::size_t reserve = (wb1 + 16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values per step
			const uint8_t *DUCKDB_BITPACKING_RESTRICT p = base + s * WIDTH;
			duckdb_av_u8x16 w0, w1;
			std::memcpy(&w0, p, 16);
			std::memcpy(&w1, p + wb1, 16);
			duckdb_av_u32x4 lo = ShuffleDecode<duckdb_av_u32x4, WIDTH, 0, 0>(w0, zero);
			duckdb_av_u32x4 hi = ShuffleDecode<duckdb_av_u32x4, WIDTH, 4, wb1>(w1, zero);
			duckdb_av_u64x4 wlo = __builtin_convertvector(lo, duckdb_av_u64x4) + fr;
			duckdb_av_u64x4 whi = __builtin_convertvector(hi, duckdb_av_u64x4) + fr;
			std::memcpy(out + s * 8 + 0, &wlo, 32);
			std::memcpy(out + s * 8 + 4, &whi, 32);
		}
		return shuffle_groups;
	} else {
		const duckdb_av_u64x2 fr = duckdb_av_u64x2 {} + frame;
		constexpr std::size_t window = (6 * width) / 8 + (WIDTH >= 57 ? 16 : 0);
		constexpr std::size_t reserve = (window + 16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		uint64_t *DUCKDB_BITPACKING_RESTRICT out64 = reinterpret_cast<uint64_t *>(out);
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values per step, four lane pairs
			const uint8_t *DUCKDB_BITPACKING_RESTRICT p = base + s * WIDTH;
			if constexpr (WIDTH >= 57) {
				duckdb_av_u64x2 v0 = ShuffleDecode64x2<WIDTH, 0>(p, fr);
				duckdb_av_u64x2 v1 = ShuffleDecode64x2<WIDTH, 2>(p, fr);
				duckdb_av_u64x2 v2 = ShuffleDecode64x2<WIDTH, 4>(p, fr);
				duckdb_av_u64x2 v3 = ShuffleDecode64x2<WIDTH, 6>(p, fr);
				std::memcpy(out64 + s * 8 + 0, &v0, 16);
				std::memcpy(out64 + s * 8 + 2, &v1, 16);
				std::memcpy(out64 + s * 8 + 4, &v2, 16);
				std::memcpy(out64 + s * 8 + 6, &v3, 16);
			} else {
				duckdb_av_u8x16 w0, w1, w2, w3;
				std::memcpy(&w0, p + (0 * WIDTH) / 8, 16);
				std::memcpy(&w1, p + (2 * WIDTH) / 8, 16);
				std::memcpy(&w2, p + (4 * WIDTH) / 8, 16);
				std::memcpy(&w3, p + (6 * WIDTH) / 8, 16);
				duckdb_av_u64x2 v0 = ShuffleDecode<duckdb_av_u64x2, WIDTH, 0, (0 * WIDTH) / 8>(w0, fr);
				duckdb_av_u64x2 v1 = ShuffleDecode<duckdb_av_u64x2, WIDTH, 2, (2 * WIDTH) / 8>(w1, fr);
				duckdb_av_u64x2 v2 = ShuffleDecode<duckdb_av_u64x2, WIDTH, 4, (4 * WIDTH) / 8>(w2, fr);
				duckdb_av_u64x2 v3 = ShuffleDecode<duckdb_av_u64x2, WIDTH, 6, (6 * WIDTH) / 8>(w3, fr);
				std::memcpy(out64 + s * 8 + 0, &v0, 16);
				std::memcpy(out64 + s * 8 + 2, &v1, 16);
				std::memcpy(out64 + s * 8 + 4, &v2, 16);
				std::memcpy(out64 + s * 8 + 6, &v3, 16);
			}
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
