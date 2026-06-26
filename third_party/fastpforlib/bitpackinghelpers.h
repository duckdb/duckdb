 /**
-* This code is released under the
-* Apache License Version 2.0 http://www.apache.org/licenses/.
-*
-* (c) Daniel Lemire, http://lemire.me/en/
-*/
#pragma once
#include "bitpacking.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace duckdb_fastpforlib {
namespace internal {

static constexpr const uint32_t BITPACKING_GROUP_SIZE = 32;

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

template <uint32_t WIDTH, class OUT, std::size_t INDEX>
static inline void UnpackValue(const uint32_t *__restrict in, OUT *__restrict out) {
	static_assert(WIDTH <= sizeof(OUT) * 8, "Bitpacking width exceeds output type width");
	constexpr const std::size_t bit_position = INDEX * WIDTH;
	constexpr const std::size_t word_index = bit_position / 32;
	constexpr const uint32_t shift = bit_position % 32;

	if constexpr (WIDTH <= 32) {
		constexpr const uint32_t mask = WIDTH == 32 ? ~uint32_t(0) : uint32_t(Mask<WIDTH>());
		if constexpr (shift + WIDTH <= 32) {
			out[INDEX] = OUT((in[word_index] >> shift) & mask);
		} else {
			const uint32_t value = (in[word_index] >> shift) | (in[word_index + 1] << (32 - shift));
			out[INDEX] = OUT(value & mask);
		}
	} else {
		constexpr const uint64_t mask = Mask<WIDTH>();
		if constexpr (shift == 0) {
			const uint64_t value = uint64_t(in[word_index]) | (uint64_t(in[word_index + 1]) << 32);
			out[INDEX] = OUT(value & mask);
		} else {
			uint64_t value = uint64_t(in[word_index]) >> shift;
			value |= uint64_t(in[word_index + 1]) << (32 - shift);
			if constexpr (shift + WIDTH > 64) {
				value |= uint64_t(in[word_index + 2]) << (64 - shift);
			}
			out[INDEX] = OUT(value & mask);
		}
	}
}

template <uint32_t WIDTH, class OUT, std::size_t... INDEXES>
static inline void UnpackBlockInternal(const uint32_t *__restrict in, OUT *__restrict out,
                                       std::index_sequence<INDEXES...>) {
	(UnpackValue<WIDTH, OUT, INDEXES>(in, out), ...);
}

template <class OUT>
static inline void Unpack4(const uint32_t *__restrict in, OUT *__restrict out) {
	auto src = reinterpret_cast<const uint8_t *>(in);
#if defined(__clang__)
#pragma clang loop vectorize_width(16) interleave_count(1)
#endif
	for (uint8_t i = 0; i < 16; i++) {
		const uint8_t value = src[i];
		out[2 * i + 0] = OUT(value & 0x0F);
		out[2 * i + 1] = OUT(value >> 4);
	}
}

template <class OUT>
static inline void Unpack12(const uint32_t *__restrict in, OUT *__restrict out) {
	auto src = reinterpret_cast<const uint16_t *>(in);
#if defined(__clang__)
#pragma clang loop vectorize_width(8) interleave_count(1)
#endif
	for (uint16_t group = 0; group < 8; group++) {
		const uint16_t u0 = src[3 * group + 0];
		const uint16_t u1 = src[3 * group + 1];
		const uint16_t u2 = src[3 * group + 2];
		out[4 * group + 0] = OUT((u0 & 0x0FFF));
		out[4 * group + 1] = OUT((u0 >> 12) | ((u1 & 0x00FF) << 4));
		out[4 * group + 2] = OUT((u1 >>  8) | ((u2 & 0x000F) << 8));
		out[4 * group + 3] = OUT((u2 >>  4));
	}
}

template <class OUT>
static inline void Unpack24(const uint32_t *__restrict in, OUT *__restrict out) {
#if defined(__clang__)
#pragma clang loop vectorize_width(4) interleave_count(1)
#endif
	for (uint32_t group = 0; group < 8; group++) {
		const uint32_t u0 = in[3 * group + 0];
		const uint32_t u1 = in[3 * group + 1];
		const uint32_t u2 = in[3 * group + 2];
		out[4 * group + 0] = OUT((u0 & 0x00FFFFFF));
		out[4 * group + 1] = OUT((u0 >> 24) | ((u1 & 0x0000FFFFu) <<  8));
		out[4 * group + 2] = OUT((u1 >> 16) | ((u2 & 0x000000FFu) << 16));
		out[4 * group + 3] = OUT((u2 >>  8));
	}
}

template <uint32_t WIDTH, class OUT>
static inline void UnpackBlock(const uint32_t *__restrict in, OUT *__restrict out) {
	if constexpr (WIDTH == 0) {
		std::memset(out, 0, BITPACKING_GROUP_SIZE * sizeof(OUT));
	} else if constexpr (WIDTH == 4 && (std::is_same<OUT, uint8_t>::value || std::is_same<OUT, uint64_t>::value)) {
		Unpack4(in, out);
	} else if constexpr (WIDTH == 12 && (std::is_same<OUT, uint16_t>::value ||
	                                     std::is_same<OUT, uint32_t>::value ||
	                                     std::is_same<OUT, uint64_t>::value)) {
		Unpack12(in, out);
	} else if constexpr (WIDTH == 24 && (std::is_same<OUT, uint32_t>::value ||
	                                     std::is_same<OUT, uint64_t>::value)) {
		Unpack24(in, out);
	} else if constexpr (WIDTH == 8 * sizeof(OUT)) {
		std::memcpy(out, in, BITPACKING_GROUP_SIZE * sizeof(OUT));
	} else {
		UnpackBlockInternal<WIDTH, OUT>(in, out, std::make_index_sequence<BITPACKING_GROUP_SIZE> {});
	}
}

template <uint32_t WIDTH, class OUT>
static inline void UnpackBuffer(const uint32_t *__restrict in, OUT *__restrict out, std::size_t groups) {
	for (std::size_t group = 0; group < groups; group++) {
		UnpackBlock<WIDTH, OUT>(in + group * WIDTH, out + group * BITPACKING_GROUP_SIZE);
	}
}

template <uint32_t WIDTH, class IN, std::size_t INDEX>
static inline void PackValue(const IN *__restrict in, uint32_t *__restrict out) {
	static_assert(WIDTH <= sizeof(IN) * 8, "Bitpacking width exceeds input type width");
	if constexpr (WIDTH == 0) {
		return;
	} else {
		constexpr const std::size_t bit_position = INDEX * WIDTH;
		constexpr const std::size_t word_index = bit_position / 32;
		constexpr const uint32_t shift = bit_position % 32;
		const uint64_t value = uint64_t(in[INDEX]) & Mask<WIDTH>();

		if constexpr (shift == 0) {
			out[word_index] |= uint32_t(value);
			if constexpr (WIDTH > 32) {
				out[word_index + 1] |= uint32_t(value >> 32);
			}
		} else {
			out[word_index] |= uint32_t(value << shift);
			if constexpr (shift + WIDTH > 32) {
				out[word_index + 1] |= uint32_t(value >> (32 - shift));
			}
			if constexpr (shift + WIDTH > 64) {
				out[word_index + 2] |= uint32_t(value >> (64 - shift));
			}
		}
	}
}

template <uint32_t WIDTH, class IN, std::size_t... INDEXES>
static inline void PackBlockInternal(const IN *__restrict in, uint32_t *__restrict out,
                                     std::index_sequence<INDEXES...>) {
	(PackValue<WIDTH, IN, INDEXES>(in, out), ...);
}

template <uint32_t WIDTH, class IN>
static inline void PackBlock(const IN *__restrict in, uint32_t *__restrict out) {
	if constexpr (WIDTH == 0) {
		return;
	} else if constexpr (WIDTH == 8 * sizeof(IN)) {
		std::memcpy(out, in, BITPACKING_GROUP_SIZE * sizeof(IN));
	} else {
		std::memset(out, 0, WIDTH * sizeof(uint32_t));
		PackBlockInternal<WIDTH, IN>(in, out, std::make_index_sequence<BITPACKING_GROUP_SIZE> {});
	}
}

template <uint32_t WIDTH, class IN>
static inline void PackBuffer(const IN *__restrict in, uint32_t *__restrict out, std::size_t groups) {
	for (std::size_t group = 0; group < groups; group++) {
		PackBlock<WIDTH, IN>(in + group * BITPACKING_GROUP_SIZE, out + group * WIDTH);
	}
}

template <uint32_t MAX_WIDTH, class FUNC>
static inline void DispatchWidth(uint32_t width, FUNC &&func) {
#define DUCKDB_BITPACKING_WIDTH_CASE(WIDTH_VALUE)                                                                      \
	case WIDTH_VALUE:                                                                                                  \
		if constexpr (WIDTH_VALUE <= MAX_WIDTH) {                                                                      \
			func(std::integral_constant<uint32_t, WIDTH_VALUE> {});                                                    \
			return;                                                                                                    \
		}                                                                                                              \
		break

	switch (width) {
		DUCKDB_BITPACKING_WIDTH_CASE(0);
		DUCKDB_BITPACKING_WIDTH_CASE(1);
		DUCKDB_BITPACKING_WIDTH_CASE(2);
		DUCKDB_BITPACKING_WIDTH_CASE(3);
		DUCKDB_BITPACKING_WIDTH_CASE(4);
		DUCKDB_BITPACKING_WIDTH_CASE(5);
		DUCKDB_BITPACKING_WIDTH_CASE(6);
		DUCKDB_BITPACKING_WIDTH_CASE(7);
		DUCKDB_BITPACKING_WIDTH_CASE(8);
		DUCKDB_BITPACKING_WIDTH_CASE(9);
		DUCKDB_BITPACKING_WIDTH_CASE(10);
		DUCKDB_BITPACKING_WIDTH_CASE(11);
		DUCKDB_BITPACKING_WIDTH_CASE(12);
		DUCKDB_BITPACKING_WIDTH_CASE(13);
		DUCKDB_BITPACKING_WIDTH_CASE(14);
		DUCKDB_BITPACKING_WIDTH_CASE(15);
		DUCKDB_BITPACKING_WIDTH_CASE(16);
		DUCKDB_BITPACKING_WIDTH_CASE(17);
		DUCKDB_BITPACKING_WIDTH_CASE(18);
		DUCKDB_BITPACKING_WIDTH_CASE(19);
		DUCKDB_BITPACKING_WIDTH_CASE(20);
		DUCKDB_BITPACKING_WIDTH_CASE(21);
		DUCKDB_BITPACKING_WIDTH_CASE(22);
		DUCKDB_BITPACKING_WIDTH_CASE(23);
		DUCKDB_BITPACKING_WIDTH_CASE(24);
		DUCKDB_BITPACKING_WIDTH_CASE(25);
		DUCKDB_BITPACKING_WIDTH_CASE(26);
		DUCKDB_BITPACKING_WIDTH_CASE(27);
		DUCKDB_BITPACKING_WIDTH_CASE(28);
		DUCKDB_BITPACKING_WIDTH_CASE(29);
		DUCKDB_BITPACKING_WIDTH_CASE(30);
		DUCKDB_BITPACKING_WIDTH_CASE(31);
		DUCKDB_BITPACKING_WIDTH_CASE(32);
		DUCKDB_BITPACKING_WIDTH_CASE(33);
		DUCKDB_BITPACKING_WIDTH_CASE(34);
		DUCKDB_BITPACKING_WIDTH_CASE(35);
		DUCKDB_BITPACKING_WIDTH_CASE(36);
		DUCKDB_BITPACKING_WIDTH_CASE(37);
		DUCKDB_BITPACKING_WIDTH_CASE(38);
		DUCKDB_BITPACKING_WIDTH_CASE(39);
		DUCKDB_BITPACKING_WIDTH_CASE(40);
		DUCKDB_BITPACKING_WIDTH_CASE(41);
		DUCKDB_BITPACKING_WIDTH_CASE(42);
		DUCKDB_BITPACKING_WIDTH_CASE(43);
		DUCKDB_BITPACKING_WIDTH_CASE(44);
		DUCKDB_BITPACKING_WIDTH_CASE(45);
		DUCKDB_BITPACKING_WIDTH_CASE(46);
		DUCKDB_BITPACKING_WIDTH_CASE(47);
		DUCKDB_BITPACKING_WIDTH_CASE(48);
		DUCKDB_BITPACKING_WIDTH_CASE(49);
		DUCKDB_BITPACKING_WIDTH_CASE(50);
		DUCKDB_BITPACKING_WIDTH_CASE(51);
		DUCKDB_BITPACKING_WIDTH_CASE(52);
		DUCKDB_BITPACKING_WIDTH_CASE(53);
		DUCKDB_BITPACKING_WIDTH_CASE(54);
		DUCKDB_BITPACKING_WIDTH_CASE(55);
		DUCKDB_BITPACKING_WIDTH_CASE(56);
		DUCKDB_BITPACKING_WIDTH_CASE(57);
		DUCKDB_BITPACKING_WIDTH_CASE(58);
		DUCKDB_BITPACKING_WIDTH_CASE(59);
		DUCKDB_BITPACKING_WIDTH_CASE(60);
		DUCKDB_BITPACKING_WIDTH_CASE(61);
		DUCKDB_BITPACKING_WIDTH_CASE(62);
		DUCKDB_BITPACKING_WIDTH_CASE(63);
		DUCKDB_BITPACKING_WIDTH_CASE(64);
	default:
		break;
	}

#undef DUCKDB_BITPACKING_WIDTH_CASE
	throw std::logic_error("Invalid bit width for bitpacking");
}

} // namespace internal

inline void fastunpack(const uint8_t *__restrict in, uint8_t *__restrict out, const uint32_t bit,
                       const std::size_t groups = 1) {
	internal::DispatchWidth<8>(bit, [&](auto width) {
		internal::UnpackBuffer<decltype(width)::value>(reinterpret_cast<const uint32_t *>(in), out, groups);
	});
}

inline void fastunpack(const uint16_t *__restrict in, uint16_t *__restrict out, const uint32_t bit,
                       const std::size_t groups = 1) {
	internal::DispatchWidth<16>(bit, [&](auto width) {
		internal::UnpackBuffer<decltype(width)::value>(reinterpret_cast<const uint32_t *>(in), out, groups);
	});
}

inline void fastunpack(const uint32_t *__restrict in, uint32_t *__restrict out, const uint32_t bit,
                       const std::size_t groups = 1) {
	internal::DispatchWidth<32>(bit, [&](auto width) {
		internal::UnpackBuffer<decltype(width)::value>(in, out, groups);
	});
}

inline void fastunpack(const uint32_t *__restrict in, uint64_t *__restrict out, const uint32_t bit,
                       const std::size_t groups = 1) {
	internal::DispatchWidth<64>(bit, [&](auto width) {
		internal::UnpackBuffer<decltype(width)::value>(in, out, groups);
	});
}

inline void fastpack(const uint8_t *__restrict in, uint8_t *__restrict out, const uint32_t bit,
                     const std::size_t groups = 1) {
	internal::DispatchWidth<8>(bit, [&](auto width) {
		internal::PackBuffer<decltype(width)::value>(in, reinterpret_cast<uint32_t *>(out), groups);
	});
}

inline void fastpack(const uint16_t *__restrict in, uint16_t *__restrict out, const uint32_t bit,
                     const std::size_t groups = 1) {
	internal::DispatchWidth<16>(bit, [&](auto width) {
		internal::PackBuffer<decltype(width)::value>(in, reinterpret_cast<uint32_t *>(out), groups);
	});
}

inline void fastpack(const uint32_t *__restrict in, uint32_t *__restrict out, const uint32_t bit,
                     const std::size_t groups = 1) {
	internal::DispatchWidth<32>(bit, [&](auto width) {
		internal::PackBuffer<decltype(width)::value>(in, out, groups);
	});
}

inline void fastpack(const uint64_t *__restrict in, uint32_t *__restrict out, const uint32_t bit,
                     const std::size_t groups = 1) {
	internal::DispatchWidth<64>(bit, [&](auto width) {
		internal::PackBuffer<decltype(width)::value>(in, out, groups);
	});
}

} // namespace duckdb_fastpforlib
