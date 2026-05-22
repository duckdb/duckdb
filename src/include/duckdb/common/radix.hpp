//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/radix.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/limits.hpp"

#include <cstring> // strlen() on Solaris
#include <limits>

namespace duckdb {

struct Radix {
private:
	template <class WORD_TYPE, idx_t exponent_bits, idx_t fraction_bits>
	struct RadixKeyConstants {
	public:
		using WORD = WORD_TYPE;
		static constexpr idx_t WORD_BITS = sizeof(WORD) * 8;
		static constexpr idx_t EXPONENT_BITS = exponent_bits;
		static constexpr idx_t FRACTION_BITS = fraction_bits;

		static_assert(WORD_BITS == EXPONENT_BITS + FRACTION_BITS + 1,
		              "Key constants must describe an IEEE 754 binary format");
		static constexpr WORD SIGN_BIT = WORD(1) << (WORD_BITS - 1);
		static constexpr WORD ABS_MASK = SIGN_BIT - 1;
		static constexpr WORD EXPONENT_MASK = ((WORD(1) << EXPONENT_BITS) - 1) << FRACTION_BITS;

		static constexpr WORD POSITIVE_ZERO_BITS = WORD(0);
		static constexpr WORD NEGATIVE_ZERO_BITS = SIGN_BIT;

	public:
		// Map raw floating-point bits into the unsigned comparison order.
		static constexpr WORD EncodeSignAwareKeyBits(WORD bits) {
			const auto sign_mask = WORD(0) - (bits >> (WORD_BITS - 1));
			// Positive numbers just have the sign bit flipped, moving them into the upper
			// part of unsigned integer space.
			// Negative numbers have every bit inverted, moving them into the lower part
			// of unsigned integer space with reversed order, so higher magnitude negative
			// values sort before lower magnitude negative values.
			return bits ^ (SIGN_BIT | sign_mask);
		}

		static constexpr WORD ART_ORDERED_NAN_RADIX_KEY = ~WORD(0);
		static constexpr WORD ART_ORDERED_NEGATIVE_INFINITY_RADIX_KEY = WORD(0);
		static constexpr WORD ART_ORDERED_POSITIVE_ZERO_RADIX_KEY = EncodeSignAwareKeyBits(POSITIVE_ZERO_BITS);
		static constexpr WORD ART_ORDERED_POSITIVE_INFINITY_RADIX_KEY = ART_ORDERED_NAN_RADIX_KEY - 1;

		static constexpr WORD MINMAX_NEGATIVE_ZERO_RADIX_KEY = EncodeSignAwareKeyBits(NEGATIVE_ZERO_BITS);
	};

	using FloatKeyConstants = RadixKeyConstants<uint32_t, 8, 23>;
	using DoubleKeyConstants = RadixKeyConstants<uint64_t, 11, 52>;

	template <class WORD>
	static inline WORD BooleanMask(bool value) {
		return WORD(0) - static_cast<WORD>(value);
	}

	template <class WORD>
	static inline WORD SelectByMask(WORD true_value, WORD false_value, WORD mask) {
		return (true_value & mask) | (false_value & ~mask);
	}

	template <class CONSTANTS>
	static inline typename CONSTANTS::WORD EncodeKeyBits(typename CONSTANTS::WORD bits) {
		using WORD = typename CONSTANTS::WORD;
		auto key = CONSTANTS::EncodeSignAwareKeyBits(bits);

		// Clear the sign bit, leaving the exponent and fractional parts
		const auto abs_bits = bits & CONSTANTS::ABS_MASK;

		// NaNs sort above everything and have their payload destroyed:
		// float:  NaN 0xFFFFFFFF
		// double: NaN 0xFFFFFFFFFFFFFFFF
		key |= BooleanMask<WORD>(abs_bits > CONSTANTS::EXPONENT_MASK);
		return key;
	}

	template <class CONSTANTS>
	static inline typename CONSTANTS::WORD EncodeARTOrderedKeyBits(typename CONSTANTS::WORD bits) {
		using WORD = typename CONSTANTS::WORD;
		auto key = EncodeKeyBits<CONSTANTS>(bits);

		const auto abs_bits = bits & CONSTANTS::ABS_MASK;
		const auto sign_mask = WORD(0) - (bits >> (CONSTANTS::WORD_BITS - 1));
		const auto is_infinity_mask = BooleanMask<WORD>(abs_bits == CONSTANTS::EXPONENT_MASK);
		const auto is_zero_mask = BooleanMask<WORD>(abs_bits == WORD(0));

		// Set the ART-ordered infinity radix keys:
		// float:  -inf 0x00000000,         +inf 0xFFFFFFFE
		// double: -inf 0x0000000000000000, +inf 0xFFFFFFFFFFFFFFFE
		const auto infinity_key = CONSTANTS::ART_ORDERED_POSITIVE_INFINITY_RADIX_KEY & ~sign_mask;
		key = SelectByMask<WORD>(infinity_key, key, is_infinity_mask);

		// Canonicalise -0.0 to the +0.0 ART-ordered radix key:
		// float:  -0.0 0x80000000,         +0.0 0x80000000
		// double: -0.0 0x8000000000000000, +0.0 0x8000000000000000
		key = SelectByMask<WORD>(CONSTANTS::ART_ORDERED_POSITIVE_ZERO_RADIX_KEY, key, is_zero_mask);

		return key;
	}

	template <class CONSTANTS>
	static inline typename CONSTANTS::WORD NormalizeMinMaxKeyForDecode(typename CONSTANTS::WORD key) {
		using WORD = typename CONSTANTS::WORD;
		// Sortable min/max keys keep -0.0 and +0.0 separate while reducing.
		// Canonicalise the min/max -0.0 key to the +0.0 ART-ordered radix key before decode:
		// float:  0x7FFFFFFF -> 0x80000000
		// double: 0x7FFFFFFFFFFFFFFF -> 0x8000000000000000
		return SelectByMask<WORD>(CONSTANTS::ART_ORDERED_POSITIVE_ZERO_RADIX_KEY, key,
		                          BooleanMask<WORD>(key == CONSTANTS::MINMAX_NEGATIVE_ZERO_RADIX_KEY));
	}

public:
	template <class T>
	static inline void EncodeData(data_ptr_t dataptr, T value) {
		throw NotImplementedException("Cannot create data from this type");
	}

	template <class T>
	static inline T DecodeData(const_data_ptr_t input) {
		throw NotImplementedException("Cannot read data from this type");
	}

	static inline void EncodeStringDataPrefix(data_ptr_t dataptr, string_t value, idx_t prefix_len) {
		auto len = value.GetSize();
		memcpy(dataptr, value.GetData(), MinValue(len, prefix_len));
		if (len < prefix_len) {
			memset(dataptr + len, '\0', prefix_len - len);
		}
	}

	static inline uint8_t FlipSign(uint8_t key_byte) {
		return key_byte ^ 128;
	}

	//! Encode a float value into an ART-ordered radix key.
	static inline uint32_t EncodeFloat(float x) {
		return EncodeARTOrderedKeyBits<FloatKeyConstants>(Load<uint32_t>(const_data_ptr_cast(&x)));
	}

	//! Decode a float ART-ordered radix key.
	static inline float DecodeFloat(uint32_t input) {
		if (input == FloatKeyConstants::ART_ORDERED_NAN_RADIX_KEY) {
			return std::numeric_limits<float>::quiet_NaN();
		}
		if (input == FloatKeyConstants::ART_ORDERED_POSITIVE_INFINITY_RADIX_KEY) {
			return std::numeric_limits<float>::infinity();
		}
		if (input == FloatKeyConstants::ART_ORDERED_NEGATIVE_INFINITY_RADIX_KEY) {
			return -std::numeric_limits<float>::infinity();
		}
		float result;
		if (input & FloatKeyConstants::SIGN_BIT) {
			// positive numbers - flip sign bit
			input = input ^ FloatKeyConstants::SIGN_BIT;
		} else {
			// negative numbers - invert
			input = ~input;
		}
		Store<uint32_t>(input, data_ptr_cast(&result));
		return result;
	}

	//! Encode raw float bits as a sortable min/max key.
	static inline uint32_t EncodeFloatMinMaxKeyBits(uint32_t bits) {
		// The min/max keys don't use the ART-ordered radix keys for -inf and +inf, and
		// keep -0.0 and +0.0 distinct while reducing, canonicalising zeros
		// on decode.
		return EncodeKeyBits<FloatKeyConstants>(bits);
	}

	//! Decode a float min/max key.
	static inline float DecodeFloatMinMaxKey(uint32_t key) {
		return DecodeFloat(NormalizeMinMaxKeyForDecode<FloatKeyConstants>(key));
	}

	//! Encode a double value into an ART-ordered radix key.
	static inline uint64_t EncodeDouble(double x) {
		return EncodeARTOrderedKeyBits<DoubleKeyConstants>(Load<uint64_t>(const_data_ptr_cast(&x)));
	}

	//! Decode a double ART-ordered radix key.
	static inline double DecodeDouble(uint64_t input) {
		if (input == DoubleKeyConstants::ART_ORDERED_NAN_RADIX_KEY) {
			return std::numeric_limits<double>::quiet_NaN();
		}
		if (input == DoubleKeyConstants::ART_ORDERED_POSITIVE_INFINITY_RADIX_KEY) {
			return std::numeric_limits<double>::infinity();
		}
		if (input == DoubleKeyConstants::ART_ORDERED_NEGATIVE_INFINITY_RADIX_KEY) {
			return -std::numeric_limits<double>::infinity();
		}
		double result;
		if (input & DoubleKeyConstants::SIGN_BIT) {
			// positive numbers - flip sign bit
			input = input ^ DoubleKeyConstants::SIGN_BIT;
		} else {
			// negative numbers - invert
			input = ~input;
		}
		Store<uint64_t>(input, data_ptr_cast(&result));
		return result;
	}

	//! Encode raw double bits as a sortable min/max key.
	static inline uint64_t EncodeDoubleMinMaxKeyBits(uint64_t bits) {
		// The min/max keys don't use the ART-ordered radix keys for -inf and +inf, and
		// keep -0.0 and +0.0 distinct while reducing, canonicalising zeros
		// on decode.
		return EncodeKeyBits<DoubleKeyConstants>(bits);
	}

	//! Decode a double min/max key.
	static inline double DecodeDoubleMinMaxKey(uint64_t key) {
		return DecodeDouble(NormalizeMinMaxKeyForDecode<DoubleKeyConstants>(key));
	}

private:
	template <class T>
	static void EncodeSigned(data_ptr_t dataptr, T value);
	template <class T>
	static T DecodeSigned(const_data_ptr_t input);
};

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, bool value) {
	Store<uint8_t>(value ? 1 : 0, dataptr);
}

template <class T>
void Radix::EncodeSigned(data_ptr_t dataptr, T value) {
	using UNSIGNED = typename MakeUnsigned<T>::type;
	UNSIGNED bytes;
	Store<T>(value, data_ptr_cast(&bytes));
	Store<UNSIGNED>(BSwapIfLE(bytes), dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int8_t value) {
	EncodeSigned<int8_t>(dataptr, value);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int16_t value) {
	EncodeSigned<int16_t>(dataptr, value);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int32_t value) {
	EncodeSigned<int32_t>(dataptr, value);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int64_t value) {
	EncodeSigned<int64_t>(dataptr, value);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint8_t value) {
	Store<uint8_t>(value, dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint16_t value) {
	Store<uint16_t>(BSwapIfLE(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint32_t value) {
	Store<uint32_t>(BSwapIfLE(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint64_t value) {
	Store<uint64_t>(BSwapIfLE(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, hugeint_t value) {
	EncodeData<int64_t>(dataptr, value.upper);
	EncodeData<uint64_t>(dataptr + sizeof(value.upper), value.lower);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uhugeint_t value) {
	EncodeData<uint64_t>(dataptr, value.upper);
	EncodeData<uint64_t>(dataptr + sizeof(value.upper), value.lower);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, float value) {
	uint32_t converted_value = EncodeFloat(value);
	Store<uint32_t>(BSwapIfLE(converted_value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, double value) {
	uint64_t converted_value = EncodeDouble(value);
	Store<uint64_t>(BSwapIfLE(converted_value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, interval_t value) {
	EncodeData<int32_t>(dataptr, value.months);
	dataptr += sizeof(value.months);
	EncodeData<int32_t>(dataptr, value.days);
	dataptr += sizeof(value.days);
	EncodeData<int64_t>(dataptr, value.micros);
}

template <>
inline bool Radix::DecodeData(const_data_ptr_t input) {
	return Load<uint8_t>(input) != 0;
}

template <class T>
T Radix::DecodeSigned(const_data_ptr_t input) {
	using UNSIGNED = typename MakeUnsigned<T>::type;
	UNSIGNED bytes = Load<UNSIGNED>(input);
	auto bytes_data = data_ptr_cast(&bytes);
	bytes_data[0] = FlipSign(bytes_data[0]);
	T result;
	Store<UNSIGNED>(BSwapIfLE(bytes), data_ptr_cast(&result));
	return result;
}

template <>
inline int8_t Radix::DecodeData(const_data_ptr_t input) {
	return DecodeSigned<int8_t>(input);
}

template <>
inline int16_t Radix::DecodeData(const_data_ptr_t input) {
	return DecodeSigned<int16_t>(input);
}

template <>
inline int32_t Radix::DecodeData(const_data_ptr_t input) {
	return DecodeSigned<int32_t>(input);
}

template <>
inline int64_t Radix::DecodeData(const_data_ptr_t input) {
	return DecodeSigned<int64_t>(input);
}

template <>
inline uint8_t Radix::DecodeData(const_data_ptr_t input) {
	return Load<uint8_t>(input);
}

template <>
inline uint16_t Radix::DecodeData(const_data_ptr_t input) {
	return BSwapIfLE(Load<uint16_t>(input));
}

template <>
inline uint32_t Radix::DecodeData(const_data_ptr_t input) {
	return BSwapIfLE(Load<uint32_t>(input));
}

template <>
inline uint64_t Radix::DecodeData(const_data_ptr_t input) {
	return BSwapIfLE(Load<uint64_t>(input));
}

template <>
inline hugeint_t Radix::DecodeData(const_data_ptr_t input) {
	hugeint_t result;
	result.upper = DecodeData<int64_t>(input);
	result.lower = DecodeData<uint64_t>(input + sizeof(int64_t));
	return result;
}

template <>
inline uhugeint_t Radix::DecodeData(const_data_ptr_t input) {
	uhugeint_t result;
	result.upper = DecodeData<uint64_t>(input);
	result.lower = DecodeData<uint64_t>(input + sizeof(uint64_t));
	return result;
}

template <>
inline float Radix::DecodeData(const_data_ptr_t input) {
	return DecodeFloat(BSwapIfLE(Load<uint32_t>(input)));
}

template <>
inline double Radix::DecodeData(const_data_ptr_t input) {
	return DecodeDouble(BSwapIfLE(Load<uint64_t>(input)));
}

template <>
inline interval_t Radix::DecodeData(const_data_ptr_t input) {
	interval_t result;
	result.months = DecodeData<int32_t>(input);
	result.days = DecodeData<int32_t>(input + sizeof(int32_t));
	result.micros = DecodeData<int64_t>(input + sizeof(int32_t) + sizeof(int32_t));
	return result;
}

} // namespace duckdb
