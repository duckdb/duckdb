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


#include <iostream>

namespace duckdb {


// ! TEMPORARY STREAM OVERLOAD
std::ostream& operator<<(std::ostream& stream, hugeint_t value) {
	stream << value.ToString();
	return stream;
}

using bitpacking_width_t = uint8_t;

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
			T tmp_buffer[BITPACKING_ALGORITHM_GROUP_SIZE]; // TODO maybe faster on the heap?

			count -= misaligned_count;

			for (idx_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE) {
				PackGroup<T>(dst + (i * width) / 8, src + i, width);
			}

			// Input was not aligned to BITPACKING_ALGORITHM_GROUP_SIZE, we need a copy
			if (misaligned_count) {
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
	template <class T>
	inline static bitpacking_width_t MinimumBitWidth(T value) {
		return FindMinimumBitWidth<T, BYTE_ALIGNED>(value, value);
	}

	// Overload specifically for the usage of size_t in the fsst library
	template <>
	bitpacking_width_t MinimumBitWidth<size_t>(size_t value) {
		return FindMinimumBitWidth<uint64_t, BYTE_ALIGNED>(uint64_t(value), uint64_t(value));
	}

	// Calculates the minimum required number of bits per value that can store all values
	template <class T>
	inline static bitpacking_width_t MinimumBitWidth(T *values, idx_t count) {
		return FindMinimumBitWidth<T, BYTE_ALIGNED>(values, count);
	}

	// Calculates the minimum required number of bits per value that can store all values,
	// given a predetermined minimum and maximum value of the buffer
	template <class T>
	inline static bitpacking_width_t MinimumBitWidth(T minimum, T maximum) {
		return FindMinimumBitWidth<T, BYTE_ALIGNED>(minimum, maximum);
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

		return num_to_round + BITPACKING_ALGORITHM_GROUP_SIZE - remainder;
	}

private:
	template <class T, bool round_to_next_byte = false>
	static bitpacking_width_t FindMinimumBitWidth(T *values, idx_t count) {
		T min_value = values[0];
		T max_value = values[0];

		for (idx_t i = 1; i < count; i++) {
			if (values[i] > max_value) {
				max_value = values[i];
			}

			if (NumericLimits<T>::IsSigned()) {
				if (values[i] < min_value) {
					min_value = values[i];
				}
			}
		}

		return FindMinimumBitWidth<T, round_to_next_byte>(min_value, max_value);
	}

	template <class T, bool round_to_next_byte = false>
	static bitpacking_width_t FindMinimumBitWidth(T min_value, T max_value) {
		bitpacking_width_t bitwidth;
		T value;

		if (NumericLimits<T>::IsSigned()) {
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

		if (NumericLimits<T>::IsSigned()) {
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
			if (NumericLimits<T>::IsSigned()) {
				D_ASSERT(max_value <= (T(1) << (bitwidth - 1)) - 1);
				D_ASSERT(min_value >= (-1 * ((T(1) << (bitwidth - 1)) - 1) - 1));
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
		T const mask = ((T_U)1) << (width - 1);
		for (idx_t i = 0; i < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE; ++i) {
			T value = Load<T>(dst + i * sizeof(T));
			value = value & ((((T_U)1) << width) - ((T_U)1));
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

	template<class T>
	static void UnpackSingleOut128(const uint32_t *__restrict &in, T *__restrict out, uint16_t delta, uint16_t shr) {
		if (delta + shr < 32) {
			*out = ((static_cast<T>(*in)) >> shr) % (T(1) << delta);
		}

		else if (delta + shr >= 32 && delta + shr < 64) {
			*out = static_cast<T>(*in) >> shr;
			++in;

			if (delta + shr > 32) {
				uint16_t NEXT_SHR = shr + delta - 32;
			
				*out |= static_cast<T>((*in) % (1U << NEXT_SHR)) << (32 - shr);
			}
		}
		
		else if (delta + shr >= 64 && delta + shr < 96) {
			*out = static_cast<T>(*in) >> shr;
			++in;

			*out |= static_cast<T>(*in) << (32 - shr);
			++in;

			if (delta + shr > 64) {
				uint8_t NEXT_SHR = delta + shr - 64;
				*out |= static_cast<T>((*in) % (1U << NEXT_SHR)) << (64 - shr);
			}
		}

		else if (delta + shr >= 96) {
			*out = static_cast<T>(*in) >> shr;
			++in;

			*out |= static_cast<T>(*in) << (32 - shr);
			++in;

			*out |= static_cast<T>(*in) << (64 - shr);
			++in;

			if (delta + shr > 96) {
				uint8_t NEXT_SHR = delta + shr - 96;
				*out |= static_cast<T>((*in) % (1U << NEXT_SHR)) << (96 - shr);
			}
		}
	}

	template<class T>
	static void PackSingleIn128(const T in, uint32_t *__restrict &out, uint16_t delta, uint16_t shl, T mask) {
		if (delta + shl < 32) {

			if (shl == 0) {
				*out = static_cast<uint32_t>(in & mask);
			} else {
				*out |= static_cast<uint32_t>((in & mask) << shl);
			}

		}
		else if  (delta + shl >= 32 && delta + shl < 64) {
		
			if (shl == 0) {
				*out = static_cast<uint32_t>(in & mask);
			} else {
				*out |= static_cast<uint32_t>((in & mask) << shl);
			}

			++out;

			if (delta + shl > 32) {
				*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
			}

		}

		else if (delta + shl >= 64 && delta + shl < 96) {

			if (shl == 0) {
				*out = static_cast<uint32_t>(in & mask);
			} else {
				*out |= static_cast<uint32_t>(in << shl);
			}
			++out;

			*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
			++out;

			if (delta + shl > 64) {
				*out = static_cast<uint32_t>((in & mask) >> (64 - shl));
			}
		}

		else if (delta + shl >= 96) {
			if (shl == 0) {
				*out = static_cast<uint32_t>(in & mask);
			} else {
				*out |= static_cast<uint32_t>(in << shl);
			}
			++out;

			*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
			++out;

			*out = static_cast<uint32_t>((in & mask) >> (64 - shl));
			++out;

			if (delta + shl > 96) {
				*out = static_cast<uint32_t>((in & mask) >> (96 - shl));
			}
		}
	}


	// Custom packing for hugeints
	// DELTA = width
	template<class T>
	static void UnpackSingle(const uint32_t *__restrict &in, T *__restrict out, uint16_t delta, uint16_t oindex) {

		std::cout << "Unpacking... with DELTA: " << (uint32_t)delta << ", SHR: "
			<< (uint32_t)((delta * oindex) % 32) << ", DELTA+SHR: " << (uint32_t)(delta + (delta * oindex) % 32) << std::endl;


		if (oindex == 31) {
			UnpackLast(in, out, delta);
		} else {
			UnpackSingleOut128(in, out + oindex, delta, (delta * oindex) % 32);
		}
	}

	template<class T>
	static void PackSingle(const T *__restrict in, uint32_t *__restrict &out, uint16_t delta, uint16_t oindex) {

		std::cout << "Packing " << in[oindex] << " with DELTA: " << (uint32_t)delta << ", SHL: "
			<< (uint32_t)((delta * oindex) % 32) << ", MASK: " << ((T(1) << delta) - 1) << ", DELTA+SHL: " << (uint32_t)(delta + (delta * oindex) % 32) << std::endl;

		if (oindex == 31) {
			PackLast(in, out, delta);
		} else {
			PackSingleIn128(in[oindex], out, delta, (delta * oindex) % 32, (T(1) << delta) - 1);
		}
	}

	// Final index (31)
	template<class T>
	static void UnpackLast(const uint32_t *__restrict &in, T *__restrict out, uint16_t delta) {
		uint16_t shift = (delta * 31) % 32;
		out[31] = (*in) >> shift;
		if (delta > 32) {
			++in;
			out[31] |= static_cast<T>(*in) << (32 - shift);
		}
		if (delta > 64) {
			++in;
			out[31] |= static_cast<T>(*in) << (64 - shift);
		}
		if (delta > 96) {
			++in;
			out[31] |= static_cast<T>(*in) << (96 - shift);
		}
	}

	template<class T>
	static void PackLast(const T *__restrict in, uint32_t *__restrict out, uint16_t delta) {
		uint16_t shift = (delta * 31) % 32;
		*out |= static_cast<uint32_t>(in[31] << shift); // What should happen here?
		if (delta > 32) {
			++out;
			*out = static_cast<uint32_t>(in[31] >> (32 - shift));
		}
		if (delta > 64) {
			++out;
			*out = static_cast<uint32_t>(in[31] >> (64 - shift));
		}
		if (delta > 96) {
			++out;
			*out = static_cast<uint32_t>(in[31] >> (96 - shift));
		}

	}

	template<class T>
	static void PackHugeint(const T *__restrict in, uint32_t *__restrict out, bitpacking_width_t width) {

		if (width == 0) {
			return ;
		}

		// width 32

		//? Special cases at certain widths?
		if (width == 64) {
			for (int i = 0; i < 32; ++i) {
				out[2 * i] = static_cast<uint32_t>(in[i]);
				out[2 * i + 1] = static_cast<uint32_t>(in[i] >> 32);
			}
			return ;
		}

		// width 96

		// width 128

		for (idx_t oindex = 0; oindex < BITPACKING_ALGORITHM_GROUP_SIZE; ++oindex) {
			PackSingle(in, out, width, oindex);

			std::cout << "Packed " << in[oindex] << std::endl; // STREAM OVERLOAD

		}
	}

	template<class T>
	static void UnPackHugeint(const uint32_t *__restrict in, T *__restrict out, bitpacking_width_t width) {

		if (width == 0) {
			for (uint32_t i = 0; i < 32; ++i) {
				*(out++) = 0;
			}
			return ;
		}

		if (width == 64) {
			for (int k = 0; k < 32; ++k) {
				out[k] = in[k * 2];
				out[k] |= static_cast<uint64_t>(in[k * 2 + 1]) << 32;
			}
			return ;
		}

		//? Special cases at certain widths?

		for (idx_t oindex = 0; oindex < BITPACKING_ALGORITHM_GROUP_SIZE; ++oindex) {
			UnpackSingle(in, out, width, oindex);

			std::cout << "Unpacked " << out[oindex] << std::endl;

		}

		// UnpackLast(in, out, width);
	}


	template <class T>
	static void PackGroup(data_ptr_t dst, T *values, bitpacking_width_t width) {


		std::cout << "PackGroup width: " << (uint32_t)width << std::endl;

		// packing reinterprets the integral type as it's unsigned counterpart,
		// except for hugeints which are exclusively signed (for now)
		PackGroupImpl(dst, reinterpret_cast<typename MakeUnsigned<T>::type *>(values), width);
	}

	template <class T>
	static void PackGroupImpl(data_ptr_t dst, T *values, bitpacking_width_t width) {
		throw InternalException("Unsupported type for bitpacking");
	}
	template <>
	void PackGroupImpl(data_ptr_t dst, uint8_t *values, bitpacking_width_t width) {
		duckdb_fastpforlib::fastpack(values, reinterpret_cast<uint8_t *>(dst), static_cast<uint32_t>(width));
	}
	template <>
	void PackGroupImpl(data_ptr_t dst, uint16_t *values, bitpacking_width_t width) {
		duckdb_fastpforlib::fastpack(values, reinterpret_cast<uint16_t *>(dst), static_cast<uint32_t>(width));
	}
	template <>
	void PackGroupImpl(data_ptr_t dst, uint32_t *values, bitpacking_width_t width) {
		duckdb_fastpforlib::fastpack(values, reinterpret_cast<uint32_t *>(dst), static_cast<uint32_t>(width));
	}
	template <>
	void PackGroupImpl(data_ptr_t dst, uint64_t *values, bitpacking_width_t width) {
		duckdb_fastpforlib::fastpack(values, reinterpret_cast<uint32_t *>(dst), static_cast<uint32_t>(width));
	}
	template <>
	void PackGroupImpl(data_ptr_t dst, hugeint_t *values, bitpacking_width_t width) {


		std::cout << "Packing these values:" << std::endl;
		for (idx_t i = 0; i < BITPACKING_ALGORITHM_GROUP_SIZE; ++i) {
			std::cout << '\t' << static_cast<hugeint_t>(values[i]).ToString() << std::endl;
		}


		PackHugeint(values, reinterpret_cast<uint32_t *>(dst), static_cast<uint32_t>(width));
	}

	template <class T>
	static void UnPackGroup(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width,
	                        bool skip_sign_extension = false) {

		std::cout << "UnPackGroup width: " << (uint32_t)width << std::endl;

		UnPackGroupImpl(reinterpret_cast<typename MakeUnsigned<T>::type *>(dst), src, width);

		if (NumericLimits<T>::IsSigned() && !skip_sign_extension && width > 0 && width < sizeof(T) * 8) {
			SignExtend<T>(dst, width);
		}
	}

	template <class T>
	static void UnPackGroupImpl(T *dst, data_ptr_t src, bitpacking_width_t width) {
		throw InternalException("Unsupported type for bitpacking");
	}

	template <>
	void UnPackGroupImpl(uint8_t *dst, data_ptr_t src, bitpacking_width_t width) {
		duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint8_t *>(src), dst, static_cast<uint32_t>(width));
	}
	template <>
	void UnPackGroupImpl(uint16_t *dst, data_ptr_t src, bitpacking_width_t width) {
		duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint16_t *>(src), dst, static_cast<uint32_t>(width));
	}
	template <>
	void UnPackGroupImpl(uint32_t *dst, data_ptr_t src, bitpacking_width_t width) {
		duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint32_t *>(src), dst, static_cast<uint32_t>(width));
	}
	template <>
	void UnPackGroupImpl(uint64_t *dst, data_ptr_t src, bitpacking_width_t width) {
		duckdb_fastpforlib::fastunpack(reinterpret_cast<const uint32_t *>(src), dst, static_cast<uint32_t>(width));
	}
	template <>
	void UnPackGroupImpl(hugeint_t *dst, data_ptr_t src, bitpacking_width_t width) {
		UnPackHugeint(reinterpret_cast<const uint32_t *>(src), dst, width);
	}

};

} // namespace duckdb
