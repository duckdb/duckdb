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
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/limits.hpp"

#include <cfloat>
#include <cstring> // strlen() on Solaris
#include <limits.h>

namespace duckdb {

struct Radix {
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

	static inline uint32_t EncodeFloat(float x) {
		uint32_t buff;
		//! zero
		if (x == 0) {
			buff = 0;
			buff |= (1u << 31);
			return buff;
		}

		// nan
		if (Value::IsNan(x)) {
			return UINT_MAX;
		}
		//! infinity
		if (x > FLT_MAX) {
			return UINT_MAX - 1;
		}
		//! -infinity
		if (x < -FLT_MAX) {
			return 0;
		}
		buff = Load<uint32_t>(const_data_ptr_cast(&x));
		if ((buff & (1U << 31)) == 0) { //! +0 and positive numbers
			buff |= (1U << 31);
		} else {          //! negative numbers
			buff = ~buff; //! complement 1
		}

		return buff;
	}

	static inline float DecodeFloat(uint32_t input) {
		// nan
		if (input == UINT_MAX) {
			return std::numeric_limits<float>::quiet_NaN();
		}
		if (input == UINT_MAX - 1) {
			return std::numeric_limits<float>::infinity();
		}
		if (input == 0) {
			return -std::numeric_limits<float>::infinity();
		}
		float result;
		if (input & (1U << 31)) {
			// positive numbers - flip sign bit
			input = input ^ (1U << 31);
		} else {
			// negative numbers - invert
			input = ~input;
		}
		Store<uint32_t>(input, data_ptr_cast(&result));
		return result;
	}

	static inline uint64_t EncodeDouble(double x) {
		uint64_t buff;
		//! zero
		if (x == 0) {
			buff = 0;
			buff += (1ULL << 63);
			return buff;
		}
		// nan
		if (Value::IsNan(x)) {
			return ULLONG_MAX;
		}
		//! infinity
		if (x > DBL_MAX) {
			return ULLONG_MAX - 1;
		}
		//! -infinity
		if (x < -DBL_MAX) {
			return 0;
		}
		buff = Load<uint64_t>(const_data_ptr_cast(&x));
		if (buff < (1ULL << 63)) { //! +0 and positive numbers
			buff += (1ULL << 63);
		} else {          //! negative numbers
			buff = ~buff; //! complement 1
		}
		return buff;
	}
	static inline double DecodeDouble(uint64_t input) {
		// nan
		if (input == ULLONG_MAX) {
			return std::numeric_limits<double>::quiet_NaN();
		}
		if (input == ULLONG_MAX - 1) {
			return std::numeric_limits<double>::infinity();
		}
		if (input == 0) {
			return -std::numeric_limits<double>::infinity();
		}
		double result;
		if (input & (1ULL << 63)) {
			// positive numbers - flip sign bit
			input = input ^ (1ULL << 63);
		} else {
			// negative numbers - invert
			input = ~input;
		}
		Store<uint64_t>(input, data_ptr_cast(&result));
		return result;
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
