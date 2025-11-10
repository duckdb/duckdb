#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

template <class T>
T VarintDecode(const_data_ptr_t &ptr) {
	T result = 0;
	uint8_t shift = 0;
	while (true) {
		uint8_t byte;
		byte = *(ptr++);
		result |= T(byte & 127) << shift;
		if ((byte & 128) == 0) {
			break;
		}
		shift += 7;
		if (shift > sizeof(T) * 8) {
			throw std::runtime_error("Varint-decoding found too large number");
		}
	}
	return result;
}

template <class T>
uint8_t GetVarintSize(T val) {
	uint8_t res = 0;
	do {
		val >>= 7;
		res++;
	} while (val != 0);
	return res;
}

template <class T>
idx_t VarintEncode(T val, data_ptr_t ptr) {
	idx_t size = 0;
	do {
		uint8_t byte = val & 127;
		val >>= 7;
		if (val != 0) {
			byte |= 128;
		}
		*ptr = byte;
		ptr++;
		size++;
	} while (val != 0);
	return size;
}

template <class T>
idx_t VarintEncode(T val, MemoryStream &ser) {
	idx_t size = 0;
	do {
		uint8_t byte = val & 127;
		val >>= 7;
		if (val != 0) {
			byte |= 128;
		}
		ser.WriteData(&byte, sizeof(uint8_t));
		size++;
	} while (val != 0);
	return size;
}

} // namespace duckdb
