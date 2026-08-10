//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/autovec.hpp
//
// Auto-vectorization helpers for bit-unpacking and bitmap materialization.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/common/vector_size.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <utility>

#if (defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 12)) && (defined(__x86_64__) || defined(__aarch64__)) &&  \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__ && !DUCKDB_SMALLER_BINARY(autovec) // clang/gcc, little-endian x86/arm
#define DUCKDB_AUTOVEC 1
#else
#define DUCKDB_AUTOVEC 0
#endif
#if DUCKDB_AUTOVEC && defined(__x86_64__)
#define DUCKDB_AUTOVEC_TARGET __attribute__((target("avx2"))) // widened x86 kernels
#define DUCKDB_AUTOVEC_X86    1 // x86: AVX2 has no per-lane 8/16-bit shifts, high-align via multiply
#define DUCKDB_AUTOVEC_INLINE __attribute__((always_inline)) inline // absorbed by a TARGET wrapper: same ISA
#else
#define DUCKDB_AUTOVEC_TARGET
#define DUCKDB_AUTOVEC_X86    0 // NEON shifts every lane width: keep the cheaper shift/combine kernel
#define DUCKDB_AUTOVEC_INLINE inline
#endif
#if defined(_MSC_VER)
#define DUCKDB_BITPACKING_RESTRICT
#else
#define DUCKDB_BITPACKING_RESTRICT __restrict // MSVC cannot parse dependent __restrict pointers
#endif
// Pin the unroll of hot dense loops: ops with no vector instruction compile to a placement-sensitive scalar loop.
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
	return true;       // NEON is always available
#else
	static const bool enabled = __builtin_cpu_supports("avx2") && !getenv("DUCKDB_DISABLE_AVX2"); // cached
	return enabled;
#endif
}
inline bool DenseAutoVecPaysOff(size_t selected, size_t span, size_t type_width) {
	return type_width && (selected * (32 / type_width) >= span); // 0-width (nested) payloads => no autovec
}
inline bool AutoVecCountPaysOff(size_t count) {
	return count >= STANDARD_VECTOR_SIZE / 4; // widened-kernel entry overhead needs a reasonably full vector
}
//! Pack 64 selection bytes, each 0 or 1, into a bitmap word. Little-endian: byte j becomes bit j.
DUCKDB_AUTOVEC_TARGET static inline uint64_t BoolsToBits(const uint8_t *bytes) {
#if DUCKDB_AUTOVEC && defined(__x86_64__)
	typedef char duckdb_av_c8x32 __attribute__((vector_size(32))); // vpmovmskb takes 32 bytes to 32 bits
	duckdb_av_c8x32 lo, hi;
	std::memcpy(&lo, bytes, 32);
	std::memcpy(&hi, bytes + 32, 32);
	return uint32_t(__builtin_ia32_pmovmskb256((duckdb_av_c8x32)(lo == 1))) |
	       uint64_t(uint32_t(__builtin_ia32_pmovmskb256((duckdb_av_c8x32)(hi == 1)))) << 32;
#else
	uint64_t word = 0; // the multiply lands byte j on bit 56+j, so the product's top byte is that group's mask
	for (uint32_t g = 0; g < 8; g++) {
		uint64_t chunk;
		std::memcpy(&chunk, bytes + g * 8, 8);
		word |= uint64_t(uint8_t((chunk * 0x0102040810204080ULL) >> 56)) << (g * 8);
	}
	return word;
#endif
}
#if DUCKDB_AUTOVEC
typedef uint8_t duckdb_av_u8x16 __attribute__((vector_size(16)));
typedef uint16_t duckdb_av_u16x8 __attribute__((vector_size(16)));
typedef uint32_t duckdb_av_u32x4 __attribute__((vector_size(16)));
typedef uint32_t duckdb_av_u32x8 __attribute__((vector_size(32)));
typedef uint64_t duckdb_av_u64x4 __attribute__((vector_size(32)));
#if DUCKDB_AUTOVEC_X86
//! Shift lanes up by SHIFT, filling from zero: lane i becomes v[i-SHIFT], or 0 below that.
template <class VEC, uint32_t SHIFT, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline VEC ShiftLanesImpl(VEC z, VEC v, std::index_sequence<I...>) {
	constexpr int N = sizeof(VEC) / sizeof(VEC {}[0]);
	return __builtin_shufflevector(z, v, static_cast<int>(I < SHIFT ? I : N + I - SHIFT)...);
}
//! In-register prefix sum: log2(N) shift-and-add steps. Caveat: faster (1.6x) on X86, but slower (0.7x) on ARM
template <class VEC, uint32_t SHIFT = 1>
DUCKDB_AUTOVEC_TARGET static inline VEC PrefixSum(VEC v) {
	constexpr uint32_t N = sizeof(VEC) / sizeof(VEC {}[0]);
	if constexpr (SHIFT < N) {
		v += ShiftLanesImpl<VEC, SHIFT>(VEC {}, v, std::make_index_sequence<N> {});
		return PrefixSum<VEC, SHIFT * 2>(v);
	} else {
		return v;
	}
}
#endif
//! Gather each lane's window byte from BASE on; HI takes the next byte; w1 continues the window past 16 bytes.
template <class VEC, uint32_t WIDTH, uint32_t BASE, bool HI, std::size_t... I>
DUCKDB_AUTOVEC_TARGET static inline VEC GatherImpl(duckdb_av_u8x16 w0, duckdb_av_u8x16 w1, std::index_sequence<I...>) {
	constexpr std::size_t LB = sizeof(VEC {}[0]);
	constexpr uint32_t WBYTE = (BASE * WIDTH) / 8; // byte the window starts at
	return (VEC)__builtin_shufflevector(
	    w0, w1, static_cast<int>(((BASE + I / LB) * WIDTH) / 8 - WBYTE + (HI ? 1 : 0) + I % LB)...);
}
template <class VEC, uint32_t WIDTH, uint32_t BASE, bool HI = false>
DUCKDB_AUTOVEC_TARGET static inline VEC Gather(duckdb_av_u8x16 w0, duckdb_av_u8x16 w1) {
	return GatherImpl<VEC, WIDTH, BASE, HI>(w0, w1, std::make_index_sequence<sizeof(VEC)> {});
}
//! Per-lane constants from offset r = ((BASE+K)*WIDTH)%8; HI_MUL = 2^(LANEBITS-WIDTH-r), 16-bit lanes' only shift.
enum class LaneOp : uint8_t { LO_SHIFT, HI_SHIFT, HI_MUL };
template <LaneOp OP, uint32_t LANEBITS, uint32_t WIDTH>
static constexpr uint64_t LaneVal(uint32_t r) { // r stays a parameter: HI_MUL's shift is negative when unused
	if constexpr (OP == LaneOp::LO_SHIFT) {
		return r;
	} else if constexpr (OP == LaneOp::HI_SHIFT) {
		return 8 - r;
	} else {
		return uint64_t(1) << (LANEBITS - WIDTH - r);
	}
}
template <class VEC, uint32_t WIDTH, uint32_t BASE, LaneOp OP, std::size_t... K>
DUCKDB_AUTOVEC_TARGET static inline VEC LaneConstsImpl(std::index_sequence<K...>) {
	constexpr uint32_t LANEBITS = 8 * sizeof(VEC {}[0]);
	return VEC {static_cast<decltype(VEC {}[0])>(LaneVal<OP, LANEBITS, WIDTH>(((BASE + K) * WIDTH) % 8))...};
}
template <class VEC, uint32_t WIDTH, uint32_t BASE, LaneOp OP>
DUCKDB_AUTOVEC_TARGET static inline VEC LaneConsts() {
	return LaneConstsImpl<VEC, WIDTH, BASE, OP>(std::make_index_sequence<sizeof(VEC) / sizeof(VEC {}[0])> {});
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
//! One kernel per width: gather at the value byte, shift unless byte-aligned, OR the crossing byte, mask, add frame.
template <class VEC, uint32_t WIDTH, uint32_t BASE>
DUCKDB_AUTOVEC_TARGET static inline VEC ShuffleDecode(duckdb_av_u8x16 w0, duckdb_av_u8x16 w1, VEC frame) {
	constexpr uint32_t LANEBITS = 8 * sizeof(VEC {}[0]);
	VEC v = Gather<VEC, WIDTH, BASE>(w0, w1);
	if constexpr (DUCKDB_AUTOVEC_X86 && sizeof(VEC {}[0]) == 2 && WIDTH % 8 != 0) {
		static_assert(!ShuffleLaneCrosses<VEC, WIDTH, BASE>(), "high-align needs the field inside its lane");
		return ((v * LaneConsts<VEC, WIDTH, BASE, LaneOp::HI_MUL>()) >> (LANEBITS - WIDTH)) + frame;
	} else {
		if constexpr (WIDTH % 8 != 0) { // byte-aligned widths are a pure gather
			v >>= LaneConsts<VEC, WIDTH, BASE, LaneOp::LO_SHIFT>();
		}
		if constexpr (ShuffleLaneCrosses<VEC, WIDTH, BASE>()) {
			v |= Gather<VEC, WIDTH, BASE, true>(w0, w1) << LaneConsts<VEC, WIDTH, BASE, LaneOp::HI_SHIFT>();
		}
		return (v & (VEC {} + static_cast<decltype(VEC {}[0])>((uint64_t(1) << WIDTH) - 1))) + frame;
	}
}
template <class VEC, uint32_t WIDTH, uint32_t BASE>
DUCKDB_AUTOVEC_TARGET static inline VEC ShuffleDecode(duckdb_av_u8x16 w, VEC frame) { // window fits one register
	return ShuffleDecode<VEC, WIDTH, BASE>(w, w, frame);
}
template <uint32_t WIDTH, class OUT_T>
static constexpr bool UseShuffleUnpack() { // u32 at width 31 would need a second window register; widths past
	                                       // 32 are rare enough that the scalar loop is not worth the code
	return WIDTH > 0 && WIDTH <= 32 && WIDTH < 8 * sizeof(OUT_T) && !(sizeof(OUT_T) == 4 && WIDTH == 31);
}
template <uint32_t WIDTH, class OUT_T>
DUCKDB_AUTOVEC_TARGET static inline std::size_t ShuffleUnpack(const uint32_t *DUCKDB_BITPACKING_RESTRICT in,
                                                              OUT_T *DUCKDB_BITPACKING_RESTRICT out, std::size_t groups,
                                                              OUT_T frame = 0) {
	constexpr std::size_t width = WIDTH;
	const uint8_t *DUCKDB_BITPACKING_RESTRICT base = reinterpret_cast<const uint8_t *>(in);
	// a u16 width whose field spills past its lane cannot be high-aligned: decode it in 32-bit lanes
	constexpr bool wide16 = DUCKDB_AUTOVEC_X86 && sizeof(OUT_T) == 2 && ShuffleLaneCrosses<duckdb_av_u16x8, WIDTH, 0>();
	constexpr bool narrow8 =
	    sizeof(OUT_T) == 1 && !DUCKDB_AUTOVEC_X86 && !ShuffleLaneCrosses<duckdb_av_u8x16, WIDTH, 0>();
	if constexpr (narrow8 || (sizeof(OUT_T) == 2 && !wide16)) { // one window fills one vector of output lanes
		using VEC = typename std::conditional<sizeof(OUT_T) == 1, duckdb_av_u8x16, duckdb_av_u16x8>::type;
		constexpr std::size_t vals = 16 / sizeof(OUT_T);   // values per step
		constexpr std::size_t stride = (vals * WIDTH) / 8; // keep constant: (s * vals * WIDTH) / 8 cannot fold
		const VEC fr = VEC {} + static_cast<OUT_T>(frame);
		constexpr std::size_t reserve = (16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * (32 / vals); s++) {
			duckdb_av_u8x16 w;
			std::memcpy(&w, base + s * stride, 16);
			VEC v = ShuffleDecode<VEC, WIDTH, 0>(w, fr);
			std::memcpy(out + s * vals, &v, 16);
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
			duckdb_av_u16x8 a = ShuffleDecode<duckdb_av_u16x8, WIDTH, 0>(w0, fr);
			duckdb_av_u16x8 b = ShuffleDecode<duckdb_av_u16x8, WIDTH, 0>(w1, fr);
			duckdb_av_u8x16 o = __builtin_shufflevector((duckdb_av_u8x16)a, (duckdb_av_u8x16)b, 0, 2, 4, 6, 8, 10, 12,
			                                            14, 16, 18, 20, 22, 24, 26, 28, 30);
			std::memcpy(out + s * 16, &o, 16);
		}
		return shuffle_groups;
	} else if constexpr (sizeof(OUT_T) == 8 && WIDTH <= 13) { // one window feeds eight 32-bit lanes; widen last
		const duckdb_av_u32x8 zero {};
		const duckdb_av_u64x4 fr = duckdb_av_u64x4 {} + frame;
		constexpr std::size_t reserve = (16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values per step
			duckdb_av_u8x16 w;
			std::memcpy(&w, base + s * WIDTH, 16);
			duckdb_av_u32x8 v = ShuffleDecode<duckdb_av_u32x8, WIDTH, 0>(w, zero);
			duckdb_av_u32x4 l = __builtin_shufflevector(v, v, 0, 1, 2, 3);
			duckdb_av_u32x4 h = __builtin_shufflevector(v, v, 4, 5, 6, 7);
			duckdb_av_u64x4 wlo = __builtin_convertvector(l, duckdb_av_u64x4) + fr;
			duckdb_av_u64x4 whi = __builtin_convertvector(h, duckdb_av_u64x4) + fr;
			std::memcpy(out + s * 8 + 0, &wlo, 32);
			std::memcpy(out + s * 8 + 4, &whi, 32);
		}
		return shuffle_groups;
	} else { // two windows feed two 32-bit lane groups; widths past 32 never reach here
		constexpr bool widen = sizeof(OUT_T) == 8;
		constexpr uint32_t wb1 = (4 * WIDTH) / 8;
		const duckdb_av_u32x4 fr = duckdb_av_u32x4 {} + static_cast<uint32_t>(widen ? 0 : frame);
		const duckdb_av_u64x4 fr64 = duckdb_av_u64x4 {} + static_cast<uint64_t>(widen ? frame : 0);
		constexpr std::size_t reserve = (wb1 + 16 + 4 * width - 1) / (4 * width); // safe overread margin
		const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values per step
			const uint8_t *DUCKDB_BITPACKING_RESTRICT p = base + s * WIDTH;
			duckdb_av_u8x16 w0, w1;
			std::memcpy(&w0, p, 16);
			std::memcpy(&w1, p + wb1, 16);
			duckdb_av_u32x4 lo = ShuffleDecode<duckdb_av_u32x4, WIDTH, 0>(w0, fr);
			duckdb_av_u32x4 hi = ShuffleDecode<duckdb_av_u32x4, WIDTH, 4>(w1, fr);
			if constexpr (widen) { // widen the 32-bit detour up and add the 64-bit frame
				duckdb_av_u64x4 wlo = __builtin_convertvector(lo, duckdb_av_u64x4) + fr64;
				duckdb_av_u64x4 whi = __builtin_convertvector(hi, duckdb_av_u64x4) + fr64;
				std::memcpy(out + s * 8 + 0, &wlo, 32);
				std::memcpy(out + s * 8 + 4, &whi, 32);
			} else if constexpr (sizeof(OUT_T) == 2) { // narrow the wide-lane detour back down
				duckdb_av_u16x8 o =
				    __builtin_shufflevector((duckdb_av_u16x8)lo, (duckdb_av_u16x8)hi, 0, 2, 4, 6, 8, 10, 12, 14);
				std::memcpy(out + s * 8, &o, 16);
			} else {
				std::memcpy(out + s * 8 + 0, &lo, 16);
				std::memcpy(out + s * 8 + 4, &hi, 16);
			}
		}
		return shuffle_groups;
	}
}
#endif // DUCKDB_AUTOVEC

} // namespace duckdb
