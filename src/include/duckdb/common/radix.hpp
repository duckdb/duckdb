//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bit_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"

#include <cfloat>
#include <cstring> // strlen() on Solaris
#include <limits.h>

namespace duckdb {

#define BSWAP16(x) ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                                                                     \
	((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |                               \
	            (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                                                                     \
	((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) | (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |        \
	            (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) | (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |         \
	            (((uint64_t)(x)&0x00000000ff000000ull) << 8) | (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |         \
	            (((uint64_t)(x)&0x000000000000ff00ull) << 40) | (((uint64_t)(x)&0x00000000000000ffull) << 56)))

struct Radix {
public:
	static inline bool IsLittleEndian() {
		int n = 1;
		if (*(char *)&n == 1) {
			return true;
		} else {
			return false;
		}
	}

	template <class T>
	static inline void EncodeData(data_ptr_t dataptr, T value) {
		throw NotImplementedException("Cannot create data from this type");
	}

	static inline void EncodeStringDataPrefix(data_ptr_t dataptr, string_t value, idx_t prefix_len) {
		auto len = value.GetSize();
		memcpy(dataptr, value.GetDataUnsafe(), MinValue(len, prefix_len));
		if (len < prefix_len) {
			memset(dataptr + len, '\0', prefix_len - len);
		}
	}

	static inline uint8_t FlipSign(uint8_t key_byte) {
		return key_byte ^ 128;
	}

	static inline uint32_t EncodeFloat(float x) {
		uint64_t buff;

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
		buff = Load<uint32_t>((const_data_ptr_t)&x);
		if ((buff & (1u << 31)) == 0) { //! +0 and positive numbers
			buff |= (1u << 31);
		} else {          //! negative numbers
			buff = ~buff; //! complement 1
		}

		return buff;
	}

	static inline uint64_t EncodeDouble(double x) {
		uint64_t buff;
		//! zero
		if (x == 0) {
			buff = 0;
			buff += (1ull << 63);
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
		buff = Load<uint64_t>((const_data_ptr_t)&x);
		if (buff < (1ull << 63)) { //! +0 and positive numbers
			buff += (1ull << 63);
		} else {          //! negative numbers
			buff = ~buff; //! complement 1
		}
		return buff;
	}
};

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, bool value) {
	Store<uint8_t>(value ? 1 : 0, dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int8_t value) {
	Store<uint8_t>(value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int16_t value) {
	Store<uint16_t>(BSWAP16(value), dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int32_t value) {
	Store<uint32_t>(BSWAP32(value), dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int64_t value) {
	Store<uint64_t>(BSWAP64(value), dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint8_t value) {
	Store<uint8_t>(value, dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint16_t value) {
	Store<uint16_t>(BSWAP16(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint32_t value) {
	Store<uint32_t>(BSWAP32(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint64_t value) {
	Store<uint64_t>(BSWAP64(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, hugeint_t value) {
	EncodeData<int64_t>(dataptr, value.upper);
	EncodeData<uint64_t>(dataptr + sizeof(value.upper), value.lower);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, float value) {
	uint32_t converted_value = EncodeFloat(value);
	Store<uint32_t>(BSWAP32(converted_value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, double value) {
	uint64_t converted_value = EncodeDouble(value);
	Store<uint64_t>(BSWAP64(converted_value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, interval_t value) {
	EncodeData<int32_t>(dataptr, value.months);
	dataptr += sizeof(value.months);
	EncodeData<int32_t>(dataptr, value.days);
	dataptr += sizeof(value.days);
	EncodeData<int64_t>(dataptr, value.micros);
}

} // namespace duckdb
