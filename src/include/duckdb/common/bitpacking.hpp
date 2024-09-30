//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitpacking.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "bitpackinghelpers.h"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"

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
		if (ASSUME_INPUT_ALIGNED) {
			for (idx_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE) {
				PackGroup<T>(dst + (i * width) / 8, src + i, width);
			}
		} else {
			idx_t misaligned_count = count % BITPACKING_ALGORITHM_GROUP_SIZE;
			count -= misaligned_count;
			for (idx_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE) {
				PackGroup<T>(dst + (i * width) / 8, src + i, width);
			}

			// The input is not aligned to BITPACKING_ALGORITHM_GROUP_SIZE.
			// Copy the unaligned count into a zero-initialized temporary group, and pack it.
			if (misaligned_count) {
				T tmp_buffer[BITPACKING_ALGORITHM_GROUP_SIZE] = {0};
				memcpy(tmp_buffer, src + count, misaligned_count * sizeof(T));
				PackGroup<T>(dst + (count * width) / 8, tmp_buffer, width);
			}
		}
	}

	// Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
	// Assumes both src and dst to be of the correct size
	template <class T>
	inline static void UnPackBuffer(data_ptr_t dst, data_ptr_t src, idx_t count, bitpacking_width_t width,
	                                bool skip_sign_extension = false) {

		for (idx_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE) {
			UnPackGroup<T>(dst + i * sizeof(T), src + (i * width) / 8, width, skip_sign_extension);
		}
	}

	// Packs a block of BITPACKING_ALGORITHM_GROUP_SIZE values
	template <class T>
	inline static void PackBlock(data_ptr_t dst, T *src, bitpacking_width_t width) {
		return PackGroup<T>(dst, src, width);
	}

	// Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
	template <class T>
	inline static void UnPackBlock(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width,
	                               bool skip_sign_extension = false) {
		return UnPackGroup<T>(dst, src, width, skip_sign_extension);
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
	static void SignExtend(data_ptr_t dst, bitpacking_width_t width) {
		T const mask = UnsafeNumericCast<T>(T_U(1) << (width - 1));
		for (idx_t i = 0; i < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE; ++i) {
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

	template <class T>
	static inline void PackGroup(data_ptr_t dst, T *values, bitpacking_width_t width) {
		if (std::is_same<T, int8_t>::value || std::is_same<T, uint8_t>::value) {
			duckdb_fastpforlib::fastpack(reinterpret_cast<const uint8_t *>(values), reinterpret_cast<uint8_t *>(dst),
			                             static_cast<uint32_t>(width));
		} else if (std::is_same<T, int16_t>::value || std::is_same<T, uint16_t>::value) {
			duckdb_fastpforlib::fastpack(reinterpret_cast<const uint16_t *>(values), reinterpret_cast<uint16_t *>(dst),
			                             static_cast<uint32_t>(width));
		} else if (std::is_same<T, int32_t>::value || std::is_same<T, uint32_t>::value) {
			duckdb_fastpforlib::fastpack(reinterpret_cast<const uint32_t *>(values), reinterpret_cast<uint32_t *>(dst),
			                             static_cast<uint32_t>(width));
		} else if (std::is_same<T, int64_t>::value || std::is_same<T, uint64_t>::value) {
			duckdb_fastpforlib::fastpack(reinterpret_cast<const uint64_t *>(values), reinterpret_cast<uint32_t *>(dst),
			                             static_cast<uint32_t>(width));
		} else if (std::is_same<T, hugeint_t>::value || std::is_same<T, uhugeint_t>::value) {
			HugeIntPacker::Pack(reinterpret_cast<const uhugeint_t *>(values), reinterpret_cast<uint32_t *>(dst), width);
		} else {
			throw InternalException("Unsupported type for bitpacking");
		}
	}

	template <class T>
	static inline void UnPackGroup(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width,
	                               bool skip_sign_extension = false) {
		if (std::is_same<T, int8_t>::value || std::is_same<T, uint8_t>::value) {
			duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint8_t *>(src), reinterpret_cast<uint8_t *>(dst),
			                               static_cast<uint32_t>(width));
		} else if (std::is_same<T, int16_t>::value || std::is_same<T, uint16_t>::value) {
			duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint16_t *>(src), reinterpret_cast<uint16_t *>(dst),
			                               static_cast<uint32_t>(width));
		} else if (std::is_same<T, int32_t>::value || std::is_same<T, uint32_t>::value) {
			duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint32_t *>(src), reinterpret_cast<uint32_t *>(dst),
			                               static_cast<uint32_t>(width));
		} else if (std::is_same<T, int64_t>::value || std::is_same<T, uint64_t>::value) {
			duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint32_t *>(src), reinterpret_cast<uint64_t *>(dst),
			                               static_cast<uint32_t>(width));
		} else if (std::is_same<T, hugeint_t>::value || std::is_same<T, uhugeint_t>::value) {
			HugeIntPacker::Unpack(reinterpret_cast<const uint32_t *>(src), reinterpret_cast<uhugeint_t *>(dst), width);
		} else {
			throw InternalException("Unsupported type for bitpacking");
		}

		if (NumericLimits<T>::IsSigned() && !skip_sign_extension && width > 0 && width < sizeof(T) * 8) {
			SignExtend<T>(dst, width);
		}
	}
};

} // namespace duckdb
