//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitpacking_kernels.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace duckdb_bitpacking {
namespace internal {

static constexpr const uint32_t BITPACKING_GROUP_SIZE = 32;

// The packed buffer is 32-bit words at an arbitrary byte offset, so access it via memcpy - a raw load/store
// would be UB on a misaligned address. A fixed-size memcpy compiles to one (un)aligned move.
template <class T>
static inline T LoadWord(const T *__restrict p) {
	T v;
	std::memcpy(&v, static_cast<const void *>(p), sizeof(T));
	return v;
}
template <class T>
static inline void StoreWord(T *__restrict p, T v) {
	std::memcpy(static_cast<void *>(p), &v, sizeof(T));
}

template <uint32_t WIDTH>
static constexpr uint64_t Mask() {
	if constexpr (WIDTH == 64) {
		return ~uint64_t(0);
	} else if constexpr (WIDTH == 0) {
		return 0;
	} else {
		return (uint64_t(1) << WIDTH) - 1;
	}
}

// Invoke f(integral_constant<size_t, I>) for I in 0..N-1, in order.
template <class F, std::size_t... I>
static inline void ForEachIndex(F &&f, std::index_sequence<I...>) {
	(f(std::integral_constant<std::size_t, I> {}), ...);
}

template <uint32_t WIDTH, class OUT, std::size_t INDEX>
static inline void UnpackValue(const uint32_t *__restrict in, OUT *__restrict out) {
	static_assert(WIDTH <= sizeof(OUT) * 8, "Bitpacking width exceeds output type width");
	constexpr std::size_t bit_position = INDEX * WIDTH;
	constexpr std::size_t word_index = bit_position / 32;
	constexpr uint32_t shift = bit_position % 32;
	if constexpr (WIDTH <= 32) {
		// uint32 arithmetic, kept even for u64 output: it auto-vectorizes markedly better than uint64.
		constexpr uint32_t mask = uint32_t(Mask<WIDTH>());
		if constexpr (shift + WIDTH <= 32) {
			out[INDEX] = OUT((LoadWord(in + word_index) >> shift) & mask);
		} else {
			const uint32_t value =
			    (LoadWord(in + word_index) >> shift) | (LoadWord(in + word_index + 1) << (32 - shift));
			out[INDEX] = OUT(value & mask);
		}
	} else {
		// WIDTH > 32 (u64 output): a value spans 2 or 3 source words.
		constexpr uint64_t mask = Mask<WIDTH>();
		uint64_t value = uint64_t(LoadWord(in + word_index)) >> shift;
		value |= uint64_t(LoadWord(in + word_index + 1)) << (32 - shift);
		if constexpr (shift + WIDTH > 64) {
			value |= uint64_t(LoadWord(in + word_index + 2)) << (64 - shift);
		}
		out[INDEX] = OUT(value & mask);
	}
}

#if defined(__clang__)
#define DUCKDB_BITPACKING_VECTORIZE _Pragma("clang loop vectorize(enable)")
// if a template instantiation fails to vectorize, make assert would trip on the warning
#pragma clang diagnostic ignored "-Wpass-failed" 
#else
#define DUCKDB_BITPACKING_VECTORIZE
#endif

#if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define DUCKDB_BITPACKING_BIG_ENDIAN 1
#else
#define DUCKDB_BITPACKING_BIG_ENDIAN 0
#endif

// Shuffle path: __builtin_shufflevector/convertvector on 128-bit vectors, which clang and GCC 12+ lower to a
// hardware byte-permute (tbl/pshufb). Little-endian only; every width has a generic fallback.
#if (defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 12)) && !DUCKDB_BITPACKING_BIG_ENDIAN
#define DUCKDB_BITPACKING_SHUFFLE 1
#else
#define DUCKDB_BITPACKING_SHUFFLE 0
#endif

constexpr std::size_t BitpackingGcd(std::size_t a, std::size_t b) {
	return b == 0 ? a : BitpackingGcd(b, a % b);
}

// Narrow-read: read packed words at the OUTPUT's granularity so load and store share a lane width, which
// auto-vectorizes better than the generic uint32-read extraction. Little-endian; WIDTH < bits so a value
// spans at most two read units.
template <uint32_t WIDTH, class OUT, std::size_t INDEX>
static inline OUT NarrowValue(const OUT *__restrict r) {
	constexpr std::size_t bits = 8 * sizeof(OUT);
	constexpr std::size_t bit_position = INDEX * WIDTH;
	constexpr std::size_t word_index = bit_position / bits;
	constexpr uint32_t shift = bit_position % bits;
	constexpr uint64_t mask = (uint64_t(1) << WIDTH) - 1;
	if constexpr (shift + WIDTH <= bits) {
		return OUT((uint64_t(LoadWord(r + word_index)) >> shift) & mask);
	} else {
		return OUT(((uint64_t(LoadWord(r + word_index)) >> shift) |
		            (uint64_t(LoadWord(r + word_index + 1)) << (bits - shift))) &
		           mask);
	}
}

template <uint32_t WIDTH, class OUT>
static inline void NarrowUnpack(const uint32_t *__restrict in, OUT *__restrict out, std::size_t groups) {
	constexpr std::size_t bits = 8 * sizeof(OUT);
	constexpr std::size_t g = BitpackingGcd(WIDTH, bits);
	constexpr std::size_t K = WIDTH / g; // read units per sub-group
	constexpr std::size_t M = bits / g;  // values per sub-group
	const OUT *__restrict r = reinterpret_cast<const OUT *>(in);
	const std::size_t subgroups = groups * (BITPACKING_GROUP_SIZE / M);
	DUCKDB_BITPACKING_VECTORIZE
	for (std::size_t s = 0; s < subgroups; s++) {
		const OUT *__restrict rr = r + s * K;
		OUT *__restrict oo = out + s * M;
		ForEachIndex([&](auto i) { oo[decltype(i)::value] = NarrowValue<WIDTH, OUT, decltype(i)::value>(rr); },
		             std::make_index_sequence<M> {});
	}
}

// Widths where NarrowUnpack beats the generic per-block path (microbenchmarked): all sub-byte widths for byte
// output; for uint16/uint32, widths sharing enough factors with the read unit to keep the sub-group small.
// Width 0 and full-width are handled by UnpackBlock (memset/memcpy).
template <uint32_t WIDTH, class OUT>
static constexpr bool UseNarrowUnpack() {
	if (WIDTH == 0 || WIDTH >= 8 * sizeof(OUT)) {
		return false;
	}
	if (sizeof(OUT) == 1) {
		return true;
	} else if (sizeof(OUT) == 2) {
		return WIDTH == 10 || WIDTH == 12 || WIDTH == 14;
	} else if (sizeof(OUT) == 4) {
		return WIDTH == 20 || WIDTH == 24;
	}
	return false;
}

template <uint32_t WIDTH, class OUT>
static inline void UnpackBlock(const uint32_t *__restrict in, OUT *__restrict out) {
	if constexpr (WIDTH == 0) {
		std::memset(out, 0, BITPACKING_GROUP_SIZE * sizeof(OUT));
	} else if constexpr (WIDTH == 8 * sizeof(OUT)) {
		std::memcpy(out, in, BITPACKING_GROUP_SIZE * sizeof(OUT));
	} else {
		ForEachIndex([&](auto i) { UnpackValue<WIDTH, OUT, decltype(i)::value>(in, out); },
		             std::make_index_sequence<BITPACKING_GROUP_SIZE> {});
	}
}

#if DUCKDB_BITPACKING_SHUFFLE
// Shuffle-based unpack: 8 values/iteration in two 128-bit groups of 4. Byte-permute each value's little-endian
// window into a 32-bit lane (one tbl/pshufb), apply a unified per-lane sub-byte shift, and mask. 128-bit only
// (GCC scalarizes 256-bit permutes). uint16: 8 values fit one window, uzp1 narrows the two u32x4 into a u16x8
// for one store. uint32: each group loads its own window and stores its u32x4 directly.
typedef uint8_t duckdb_bp_u8x16 __attribute__((vector_size(16)));
typedef uint16_t duckdb_bp_u16x8 __attribute__((vector_size(16)));
typedef uint32_t duckdb_bp_u32x4 __attribute__((vector_size(16)));

// Gather values [BASE..BASE+3] into a u32x4 from a window whose first byte is packed-byte WBYTE. Output byte I
// -> value BASE+I/4 at bit (BASE+I/4)*WIDTH; window byte = that/8 - WBYTE + I%4.
template <uint32_t WIDTH, uint32_t BASE, uint32_t WBYTE, std::size_t... I>
static inline duckdb_bp_u32x4 ShuffleGather(duckdb_bp_u8x16 w, std::index_sequence<I...>) {
	return (duckdb_bp_u32x4)__builtin_shufflevector(w, w,
	                                                static_cast<int>(((BASE + I / 4) * WIDTH) / 8 - WBYTE + I % 4)...);
}
template <uint32_t WIDTH, uint32_t BASE, std::size_t... L>
static inline duckdb_bp_u32x4 ShuffleShift(std::index_sequence<L...>) {
	return duckdb_bp_u32x4 {static_cast<uint32_t>(((BASE + L) * WIDTH) % 8)...};
}

template <uint32_t WIDTH, class OUT>
static inline void ShuffleUnpackIter(const uint8_t *__restrict base, OUT *__restrict out) {
	const duckdb_bp_u32x4 mask = duckdb_bp_u32x4 {} + static_cast<uint32_t>((uint64_t(1) << WIDTH) - 1);
	const auto seq16 = std::make_index_sequence<16> {};
	const duckdb_bp_u32x4 s0 = ShuffleShift<WIDTH, 0>(std::make_index_sequence<4> {});
	const duckdb_bp_u32x4 s4 = ShuffleShift<WIDTH, 4>(std::make_index_sequence<4> {});
	if constexpr (sizeof(OUT) == 2) {
		duckdb_bp_u8x16 w;
		std::memcpy(&w, base, 16);
		duckdb_bp_u32x4 lo = (ShuffleGather<WIDTH, 0, 0>(w, seq16) >> s0) & mask;
		duckdb_bp_u32x4 hi = (ShuffleGather<WIDTH, 4, 0>(w, seq16) >> s4) & mask;
		duckdb_bp_u16x8 o =
		    __builtin_shufflevector((duckdb_bp_u16x8)lo, (duckdb_bp_u16x8)hi, 0, 2, 4, 6, 8, 10, 12, 14);
		std::memcpy(out, &o, 16);
	} else {
		constexpr uint32_t wbyte1 = (4 * WIDTH) / 8; // group 1's window (its values span past 16 bytes)
		duckdb_bp_u8x16 w0, w1;
		std::memcpy(&w0, base, 16);
		std::memcpy(&w1, base + wbyte1, 16);
		duckdb_bp_u32x4 lo = (ShuffleGather<WIDTH, 0, 0>(w0, seq16) >> s0) & mask;
		duckdb_bp_u32x4 hi = (ShuffleGather<WIDTH, 4, wbyte1>(w1, seq16) >> s4) & mask;
		std::memcpy(out, &lo, 16);
		std::memcpy(out + 4, &hi, 16);
	}
}

// Which (WIDTH, OUT) use the shuffle path. uint16: straddling widths (gcd(WIDTH,16) <= 2). uint32: 17..31 minus
// the widths whose value can span 5 bytes ((8 - gcd(WIDTH,8)) + WIDTH > 32) that a 4-byte gather lane can't hold.
template <uint32_t WIDTH, class OUT>
static constexpr bool UseShuffleUnpack() {
	if (WIDTH == 0 || WIDTH >= 8 * sizeof(OUT)) {
		return false;
	}
	if (sizeof(OUT) == 2) {
		return WIDTH >= 2 && BitpackingGcd(WIDTH, 16) <= 2;
	} else if (sizeof(OUT) == 4) {
		return WIDTH >= 17 && (8 - BitpackingGcd(WIDTH, 8)) + WIDTH <= 32;
	}
	return false;
}

template <uint32_t WIDTH, class OUT>
static inline void ShuffleUnpack(const uint32_t *__restrict in, OUT *__restrict out, std::size_t groups) {
	// Reserve trailing groups (done via the generic path) so the windowed 16-byte loads never read past the buffer.
	constexpr std::size_t reserve = ((4 * WIDTH) / 8 + 16 + 4 * WIDTH - 1) / (4 * WIDTH);
	const std::size_t shuffle_groups = groups > reserve ? groups - reserve : 0;
	const uint8_t *__restrict base = reinterpret_cast<const uint8_t *>(in);
	for (std::size_t s = 0; s < shuffle_groups * 4; s++) { // 8 values/iteration, 4 iterations/block
		ShuffleUnpackIter<WIDTH, OUT>(base + s * WIDTH, out + s * 8);
	}
	for (std::size_t group = shuffle_groups; group < groups; group++) {
		UnpackBlock<WIDTH, OUT>(in + group * WIDTH, out + group * BITPACKING_GROUP_SIZE);
	}
}
#endif

template <uint32_t WIDTH, class OUT>
static inline void UnpackBuffer(const uint32_t *__restrict in, OUT *__restrict out, std::size_t groups) {
#if DUCKDB_BITPACKING_SHUFFLE
	if constexpr (UseShuffleUnpack<WIDTH, OUT>()) {
		ShuffleUnpack<WIDTH, OUT>(in, out, groups);
		return;
	}
#endif
	if constexpr (UseNarrowUnpack<WIDTH, OUT>() && !DUCKDB_BITPACKING_BIG_ENDIAN) {
		NarrowUnpack<WIDTH, OUT>(in, out, groups);
	} else {
		for (std::size_t group = 0; group < groups; group++) {
			UnpackBlock<WIDTH, OUT>(in + group * WIDTH, out + group * BITPACKING_GROUP_SIZE);
		}
	}
}

template <uint32_t WIDTH, class IN, std::size_t INDEX>
static inline void PackValue(const IN *__restrict in, uint32_t *__restrict out) {
	static_assert(WIDTH <= sizeof(IN) * 8, "Bitpacking width exceeds input type width");
	if constexpr (WIDTH > 0) {
		constexpr std::size_t bit_position = INDEX * WIDTH;
		constexpr std::size_t word_index = bit_position / 32;
		constexpr uint32_t shift = bit_position % 32;
		const uint64_t value = uint64_t(in[INDEX]) & Mask<WIDTH>();
		StoreWord(out + word_index, LoadWord(out + word_index) | uint32_t(value << shift));
		if constexpr (shift + WIDTH > 32) {
			StoreWord(out + word_index + 1, LoadWord(out + word_index + 1) | uint32_t(value >> (32 - shift)));
		}
		if constexpr (shift + WIDTH > 64) {
			StoreWord(out + word_index + 2, LoadWord(out + word_index + 2) | uint32_t(value >> (64 - shift)));
		}
	}
}

template <uint32_t WIDTH, class IN>
static inline void PackBlock(const IN *__restrict in, uint32_t *__restrict out) {
	if constexpr (WIDTH == 0) {
		return;
	} else if constexpr (WIDTH == 8 * sizeof(IN)) {
		std::memcpy(out, in, BITPACKING_GROUP_SIZE * sizeof(IN));
	} else {
		std::memset(out, 0, WIDTH * sizeof(uint32_t));
		ForEachIndex([&](auto i) { PackValue<WIDTH, IN, decltype(i)::value>(in, out); },
		             std::make_index_sequence<BITPACKING_GROUP_SIZE> {});
	}
}

template <uint32_t WIDTH, class IN>
static inline void PackBuffer(const IN *__restrict in, uint32_t *__restrict out, std::size_t groups) {
	for (std::size_t group = 0; group < groups; group++) {
		PackBlock<WIDTH, IN>(in + group * BITPACKING_GROUP_SIZE, out + group * WIDTH);
	}
}

// Turn the runtime width into a compile-time constant and call func(integral_constant<uint32_t, width>).
// Only widths 0..MAX_WIDTH are instantiated (so e.g. uint8 never instantiates width 9+).
template <class FUNC, std::size_t... W>
static inline bool DispatchWidthImpl(uint32_t width, FUNC &&func, std::index_sequence<W...>) {
	return ((width == W && (func(std::integral_constant<uint32_t, uint32_t(W)> {}), true)) || ...);
}
template <uint32_t MAX_WIDTH, class FUNC>
static inline void DispatchWidth(uint32_t width, FUNC &&func) {
	if (!DispatchWidthImpl(width, std::forward<FUNC>(func), std::make_index_sequence<MAX_WIDTH + 1> {})) {
		throw std::logic_error("Invalid bit width for bitpacking");
	}
}

} // namespace internal

// fastunpack: IN is the packed buffer (32-bit words), OUT the destination values. fastpack is the inverse.
#define DUCKDB_BITPACKING_FASTUNPACK(IN_T, OUT_T, MAX_WIDTH)                                                           \
	inline void fastunpack(const IN_T *__restrict in, OUT_T *__restrict out, const uint32_t bit,                       \
	                       const std::size_t groups = 1) {                                                             \
		internal::DispatchWidth<MAX_WIDTH>(bit, [&](auto width) {                                                      \
			internal::UnpackBuffer<decltype(width)::value>(reinterpret_cast<const uint32_t *>(in), out, groups);       \
		});                                                                                                            \
	}
#define DUCKDB_BITPACKING_FASTPACK(IN_T, OUT_T, MAX_WIDTH)                                                             \
	inline void fastpack(const IN_T *__restrict in, OUT_T *__restrict out, const uint32_t bit,                         \
	                     const std::size_t groups = 1) {                                                               \
		internal::DispatchWidth<MAX_WIDTH>(bit, [&](auto width) {                                                      \
			internal::PackBuffer<decltype(width)::value>(in, reinterpret_cast<uint32_t *>(out), groups);               \
		});                                                                                                            \
	}

DUCKDB_BITPACKING_FASTUNPACK(uint8_t, uint8_t, 8)
DUCKDB_BITPACKING_FASTUNPACK(uint16_t, uint16_t, 16)
DUCKDB_BITPACKING_FASTUNPACK(uint32_t, uint32_t, 32)
DUCKDB_BITPACKING_FASTUNPACK(uint32_t, uint64_t, 64)
DUCKDB_BITPACKING_FASTPACK(uint8_t, uint8_t, 8)
DUCKDB_BITPACKING_FASTPACK(uint16_t, uint16_t, 16)
DUCKDB_BITPACKING_FASTPACK(uint32_t, uint32_t, 32)
DUCKDB_BITPACKING_FASTPACK(uint64_t, uint32_t, 64)

#undef DUCKDB_BITPACKING_FASTUNPACK
#undef DUCKDB_BITPACKING_FASTPACK

} // namespace duckdb_bitpacking
