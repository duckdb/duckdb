//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitpacking.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/autovec.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"

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
static inline T LoadWord(const T *DUCKDB_BITPACKING_RESTRICT p) {
	T v;
	std::memcpy(&v, static_cast<const void *>(p), sizeof(T));
	return v;
}
template <class T>
static inline void StoreWord(T *DUCKDB_BITPACKING_RESTRICT p, T v) {
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

// Extract value INDEX of a 32-value group. For outputs up to 32 bits we read at the OUTPUT element width so
// load and store share a lane width (auto-vectorizes); a value then spans at most two read units. The 64-bit
// output is the one special case: the packing is 32-bit words, so a value spans 2-3 of them.
template <uint32_t WIDTH, class OUT_T, std::size_t INDEX>
static inline void UnpackValue(const uint32_t *DUCKDB_BITPACKING_RESTRICT in, OUT_T *DUCKDB_BITPACKING_RESTRICT out,
                               OUT_T frame) {
	static_assert(WIDTH <= sizeof(OUT_T) * 8, "Bitpacking width exceeds output type width");
	if constexpr (sizeof(OUT_T) <= 4) {
		constexpr std::size_t bits = 8 * sizeof(OUT_T);
		constexpr std::size_t bit_position = INDEX * WIDTH;
		constexpr std::size_t word_index = bit_position / bits;
		constexpr uint32_t shift = bit_position % bits;
		constexpr uint32_t mask = uint32_t(Mask<WIDTH>());
		const OUT_T *DUCKDB_BITPACKING_RESTRICT r = reinterpret_cast<const OUT_T *>(in);
		if constexpr (shift + WIDTH <= bits) {
			out[INDEX] = OUT_T(OUT_T((uint32_t(LoadWord(r + word_index)) >> shift) & mask) + frame);
		} else {
			out[INDEX] = OUT_T(OUT_T(((uint32_t(LoadWord(r + word_index)) >> shift) |
			                          (uint32_t(LoadWord(r + word_index + 1)) << (bits - shift))) &
			                         mask) +
			                   frame);
		}
	} else {
		// u64 output: a value spans 2 or 3 of the 32-bit source words.
		constexpr std::size_t bit_position = INDEX * WIDTH;
		constexpr std::size_t word_index = bit_position / 32;
		constexpr uint32_t shift = bit_position % 32;
		constexpr uint64_t mask = Mask<WIDTH>();
		uint64_t value = uint64_t(LoadWord(in + word_index)) >> shift;
		value |= uint64_t(LoadWord(in + word_index + 1)) << (32 - shift);
		if constexpr (shift + WIDTH > 64) {
			value |= uint64_t(LoadWord(in + word_index + 2)) << (64 - shift);
		}
		out[INDEX] = OUT_T(OUT_T(value & mask) + frame);
	}
}

template <uint32_t WIDTH, class OUT_T>
static inline void UnpackBlock(const uint32_t *DUCKDB_BITPACKING_RESTRICT in, OUT_T *DUCKDB_BITPACKING_RESTRICT out,
                               OUT_T frame = 0) {
	if constexpr (WIDTH == 0) {
		for (uint32_t i = 0; i < BITPACKING_GROUP_SIZE; i++) {
			out[i] = frame;
		}
	} else if constexpr (WIDTH == 8 * sizeof(OUT_T)) {
		std::memcpy(out, in, BITPACKING_GROUP_SIZE * sizeof(OUT_T));
		if (frame) {
			for (uint32_t i = 0; i < BITPACKING_GROUP_SIZE; i++) {
				out[i] = OUT_T(out[i] + frame);
			}
		}
	} else {
		ForEachIndex([&](auto i) { UnpackValue<WIDTH, OUT_T, decltype(i)::value>(in, out, frame); },
		             std::make_index_sequence<BITPACKING_GROUP_SIZE> {});
	}
}

template <uint32_t WIDTH, class OUT_T>
static inline void UnpackBuffer(const uint32_t *DUCKDB_BITPACKING_RESTRICT in, OUT_T *DUCKDB_BITPACKING_RESTRICT out,
                                std::size_t groups, OUT_T frame = 0) {
	std::size_t start = 0; // groups the autovec shuffle path already unpacked (0 when it is unavailable/ineligible)
#if DUCKDB_AUTOVEC
	if constexpr (UseShuffleUnpack<WIDTH, OUT_T>()) {
		if (::duckdb::CpuBenefitsFromAutoVec()) {
			start = ShuffleUnpack<WIDTH, OUT_T>(in, out, groups, frame);
		}
	}
#endif
	for (std::size_t group = start; group < groups; group++) {
		UnpackBlock<WIDTH, OUT_T>(in + group * WIDTH, out + group * BITPACKING_GROUP_SIZE, frame);
	}
}

template <uint32_t WIDTH, class IN_T, std::size_t INDEX>
static inline void PackValue(const IN_T *DUCKDB_BITPACKING_RESTRICT in, uint32_t *DUCKDB_BITPACKING_RESTRICT out) {
	static_assert(WIDTH <= sizeof(IN_T) * 8, "Bitpacking width exceeds input type width");
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

template <uint32_t WIDTH, class IN_T>
static inline void PackBlock(const IN_T *DUCKDB_BITPACKING_RESTRICT in, uint32_t *DUCKDB_BITPACKING_RESTRICT out) {
	if constexpr (WIDTH == 0) {
		return;
	} else if constexpr (WIDTH == 8 * sizeof(IN_T)) {
		std::memcpy(out, in, BITPACKING_GROUP_SIZE * sizeof(IN_T));
	} else {
		std::memset(out, 0, WIDTH * sizeof(uint32_t));
		ForEachIndex([&](auto i) { PackValue<WIDTH, IN_T, decltype(i)::value>(in, out); },
		             std::make_index_sequence<BITPACKING_GROUP_SIZE> {});
	}
}

template <uint32_t WIDTH, class IN_T>
static inline void PackBuffer(const IN_T *DUCKDB_BITPACKING_RESTRICT in, uint32_t *DUCKDB_BITPACKING_RESTRICT out,
                              std::size_t groups) {
	for (std::size_t group = 0; group < groups; group++) {
		PackBlock<WIDTH, IN_T>(in + group * BITPACKING_GROUP_SIZE, out + group * WIDTH);
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
	inline void fastunpack(const IN_T *DUCKDB_BITPACKING_RESTRICT in, OUT_T *DUCKDB_BITPACKING_RESTRICT out,           \
	                       const uint32_t bit, const std::size_t groups = 1, const OUT_T frame = 0) {                  \
		internal::DispatchWidth<MAX_WIDTH>(bit, [&](auto width) {                                                      \
			internal::UnpackBuffer<decltype(width)::value>(reinterpret_cast<const uint32_t *>(in), out, groups,        \
			                                               frame);                                                     \
		});                                                                                                            \
	}
#define DUCKDB_BITPACKING_FASTPACK(IN_T, OUT_T, MAX_WIDTH)                                                             \
	inline void fastpack(const IN_T *DUCKDB_BITPACKING_RESTRICT in, OUT_T *DUCKDB_BITPACKING_RESTRICT out,             \
	                     const uint32_t bit, const std::size_t groups = 1) {                                           \
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

template <class T>
inline bool TryFastPack(const T *DUCKDB_BITPACKING_RESTRICT in, void *DUCKDB_BITPACKING_RESTRICT out,
                        const uint32_t bit, const std::size_t groups) {
	if constexpr (std::is_same<T, int8_t>::value || std::is_same<T, uint8_t>::value) {
		fastpack(reinterpret_cast<const uint8_t *>(in), reinterpret_cast<uint8_t *>(out), bit, groups);
	} else if constexpr (std::is_same<T, int16_t>::value || std::is_same<T, uint16_t>::value) {
		fastpack(reinterpret_cast<const uint16_t *>(in), reinterpret_cast<uint16_t *>(out), bit, groups);
	} else if constexpr (std::is_same<T, int32_t>::value || std::is_same<T, uint32_t>::value) {
		fastpack(reinterpret_cast<const uint32_t *>(in), reinterpret_cast<uint32_t *>(out), bit, groups);
	} else if constexpr (std::is_same<T, int64_t>::value || std::is_same<T, uint64_t>::value) {
		fastpack(reinterpret_cast<const uint64_t *>(in), reinterpret_cast<uint32_t *>(out), bit, groups);
	} else {
		return false;
	}
	return true;
}

template <class T>
inline bool TryFastUnpack(const void *DUCKDB_BITPACKING_RESTRICT in, T *DUCKDB_BITPACKING_RESTRICT out,
                          const uint32_t bit, const std::size_t groups, const T frame = 0) {
	if constexpr (std::is_same<T, int8_t>::value || std::is_same<T, uint8_t>::value) {
		fastunpack(reinterpret_cast<const uint8_t *>(in), reinterpret_cast<uint8_t *>(out), bit, groups,
		           static_cast<uint8_t>(frame));
	} else if constexpr (std::is_same<T, int16_t>::value || std::is_same<T, uint16_t>::value) {
		fastunpack(reinterpret_cast<const uint16_t *>(in), reinterpret_cast<uint16_t *>(out), bit, groups,
		           static_cast<uint16_t>(frame));
	} else if constexpr (std::is_same<T, int32_t>::value || std::is_same<T, uint32_t>::value) {
		fastunpack(reinterpret_cast<const uint32_t *>(in), reinterpret_cast<uint32_t *>(out), bit, groups,
		           static_cast<uint32_t>(frame));
	} else if constexpr (std::is_same<T, int64_t>::value || std::is_same<T, uint64_t>::value) {
		fastunpack(reinterpret_cast<const uint32_t *>(in), reinterpret_cast<uint64_t *>(out), bit, groups,
		           static_cast<uint64_t>(frame));
	} else {
		return false;
	}
	return true;
}

} // namespace duckdb_bitpacking

namespace duckdb {

using bitpacking_width_t = uint8_t;

struct HugeIntPacker {
	static void Pack(const uhugeint_t *__restrict in, uint32_t *__restrict out, bitpacking_width_t width);
	static void Unpack(const uint32_t *__restrict in, uhugeint_t *__restrict out, bitpacking_width_t width);
};

class BitpackingPrimitives {
public:
	static constexpr const idx_t BITPACKING_ALGORITHM_GROUP_SIZE = 32;
	static constexpr const idx_t BITPACKING_HEADER_SIZE = sizeof(uint64_t);
	static constexpr const bool BYTE_ALIGNED = false;

	// To ensure enough data is available, use GetRequiredSize() to determine the correct size for dst buffer
	// Note: input should be aligned to BITPACKING_ALGORITHM_GROUP_SIZE for good performance.
	template <class T, bool ASSUME_INPUT_ALIGNED = false>
	inline static void PackBuffer(data_ptr_t dst, T *src, idx_t count, bitpacking_width_t width) {
		idx_t misaligned_count = 0;
		if (!ASSUME_INPUT_ALIGNED) {
			misaligned_count = count % BITPACKING_ALGORITHM_GROUP_SIZE;
			count -= misaligned_count;
		}
		const auto groups = static_cast<std::size_t>(count / BITPACKING_ALGORITHM_GROUP_SIZE);
		if (!duckdb_bitpacking::TryFastPack<T>(src, dst, static_cast<uint32_t>(width), groups)) {
			if (!std::is_same<T, hugeint_t>::value && !std::is_same<T, uhugeint_t>::value) {
				throw InternalException("Unsupported type for bitpacking");
			}
			for (std::size_t group = 0; group < groups; group++) {
				HugeIntPacker::Pack(reinterpret_cast<const uhugeint_t *>(src) + group * BITPACKING_ALGORITHM_GROUP_SIZE,
				                    reinterpret_cast<uint32_t *>(dst) + group * width, width);
			}
		}

		// If the input was not aligned, pack the leftover values via a zero-padded group.
		if (misaligned_count) {
			T tmp_buffer[BITPACKING_ALGORITHM_GROUP_SIZE] = {0};
			memcpy(tmp_buffer, src + count, misaligned_count * sizeof(T));
			PackBuffer<T, true>(dst + (count * width) / 8, tmp_buffer, BITPACKING_ALGORITHM_GROUP_SIZE, width);
		}
	}

	// Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
	// Assumes both src and dst to be of the correct size
	template <class T>
	//! `frame_of_reference` is added to every value during the unpack (saving a second pass over the output).
	inline static void UnPackBuffer(data_ptr_t dst, data_ptr_t src, idx_t count, bitpacking_width_t width,
	                                bool skip_sign_extension = false, T frame_of_reference = 0) {
		const auto rounded_count = RoundUpToAlgorithmGroupSize(count);
		const auto groups = static_cast<std::size_t>(rounded_count / BITPACKING_ALGORITHM_GROUP_SIZE);
		if (!duckdb_bitpacking::TryFastUnpack<T>(src, reinterpret_cast<T *>(dst), static_cast<uint32_t>(width), groups,
		                                         frame_of_reference)) {
			if (!std::is_same<T, hugeint_t>::value && !std::is_same<T, uhugeint_t>::value) {
				throw InternalException("Unsupported type for bitpacking");
			}
			for (std::size_t group = 0; group < groups; group++) {
				HugeIntPacker::Unpack(reinterpret_cast<const uint32_t *>(src) + group * width,
				                      reinterpret_cast<uhugeint_t *>(dst) + group * BITPACKING_ALGORITHM_GROUP_SIZE,
				                      width);
			}
			if (frame_of_reference != 0) {
				auto values = reinterpret_cast<uhugeint_t *>(dst);
				for (idx_t i = 0; i < rounded_count; i++) {
					values[i] += static_cast<uhugeint_t>(frame_of_reference);
				}
			}
		}

		if (NumericLimits<T>::IsSigned() && !skip_sign_extension && width > 0 && width < sizeof(T) * 8) {
			SignExtend<T>(dst, width, rounded_count);
		}
	}

	// Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
	template <class T>
	inline static void UnPackBlock(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width,
	                               bool skip_sign_extension = false) {
		return UnPackBuffer<T>(dst, src, BITPACKING_ALGORITHM_GROUP_SIZE, width, skip_sign_extension);
	}

	// Calculates the minimum required number of bits per value that can store all values
	template <class T, bool is_signed = NumericLimits<T>::IsSigned()>
	inline static bitpacking_width_t MinimumBitWidth(T value) {
		return FindMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(value, value);
	}

	// Calculates the minimum required number of bits per value that can store all values
	template <class T, bool is_signed = NumericLimits<T>::IsSigned()>
	inline static bitpacking_width_t MinimumBitWidth(T *values, idx_t count) {
		return FindMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(values, count);
	}

	// Calculates the minimum required number of bits per value that can store all values,
	// given a predetermined minimum and maximum value of the buffer
	template <class T, bool is_signed = NumericLimits<T>::IsSigned()>
	inline static bitpacking_width_t MinimumBitWidth(T minimum, T maximum) {
		return FindMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(minimum, maximum);
	}

	inline static idx_t GetRequiredSize(idx_t count, bitpacking_width_t width) {
		count = RoundUpToAlgorithmGroupSize(count);
		return ((count * width) / 8);
	}

	template <class T>
	inline static T RoundUpToAlgorithmGroupSize(T num_to_round) {
		int remainder = num_to_round % BITPACKING_ALGORITHM_GROUP_SIZE;
		if (remainder == 0) {
			return num_to_round;
		}

		return num_to_round + BITPACKING_ALGORITHM_GROUP_SIZE - NumericCast<idx_t>(remainder);
	}

private:
	template <class T, bool is_signed, bool round_to_next_byte = false>
	static bitpacking_width_t FindMinimumBitWidth(T *values, idx_t count) {
		T min_value = values[0];
		T max_value = values[0];

		for (idx_t i = 1; i < count; i++) {
			if (values[i] > max_value) {
				max_value = values[i];
			}

			if (is_signed) {
				if (values[i] < min_value) {
					min_value = values[i];
				}
			}
		}

		return FindMinimumBitWidth<T, is_signed, round_to_next_byte>(min_value, max_value);
	}

	template <class T, bool is_signed, bool round_to_next_byte = false>
	static bitpacking_width_t FindMinimumBitWidth(T min_value, T max_value) {
		bitpacking_width_t bitwidth;
		T value;

		if (is_signed) {
			if (min_value == NumericLimits<T>::Minimum()) {
				// handle special case of the minimal value, as it cannot be negated like all other values.
				return sizeof(T) * 8;
			} else {
				value = MaxValue((T)-min_value, max_value);
			}
		} else {
			value = max_value;
		}

		if (value == 0) {
			return 0;
		}

		if (is_signed) {
			bitwidth = 1;
		} else {
			bitwidth = 0;
		}

		while (value) {
			bitwidth++;
			value >>= 1;
		}

		bitwidth = GetEffectiveWidth<T>(bitwidth);

		// Assert results are correct
#ifdef DEBUG
		if (bitwidth < sizeof(T) * 8 && bitwidth != 0) {
			if (is_signed) {
				D_ASSERT(max_value <= (T(1) << (bitwidth - 1)) - 1);
				// D_ASSERT(min_value >= (T(-1) * ((T(1) << (bitwidth - 1)) - 1) - 1));
			} else {
				D_ASSERT(max_value <= (T(1) << (bitwidth)) - 1);
			}
		}
#endif
		if (round_to_next_byte) {
			return (bitwidth / 8 + (bitwidth % 8 != 0)) * 8;
		}
		return bitwidth;
	}

	// Sign bit extension
	template <class T, class T_U = typename MakeUnsigned<T>::type>
	static void SignExtend(data_ptr_t dst, bitpacking_width_t width, idx_t count) {
		T const mask = UnsafeNumericCast<T>(T_U(1) << (width - 1));
		for (idx_t i = 0; i < count; ++i) {
			T value = Load<T>(dst + i * sizeof(T));
			value = UnsafeNumericCast<T>(T_U(value) & ((T_U(1) << width) - T_U(1)));
			T result = (value ^ mask) - mask;
			Store(result, dst + i * sizeof(T));
		}
	}

	// Prevent compression at widths that are ineffective
	template <class T>
	static bitpacking_width_t GetEffectiveWidth(bitpacking_width_t width) {
		bitpacking_width_t bits_of_type = sizeof(T) * 8;
		bitpacking_width_t type_size = sizeof(T);
		if (width + type_size > bits_of_type) {
			return bits_of_type;
		}
		return width;
	}
};

} // namespace duckdb
