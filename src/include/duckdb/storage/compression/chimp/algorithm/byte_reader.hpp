//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/algorithm/byte_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

class ByteReader {
public:
	ByteReader() : buffer(nullptr), index(0) {
	}

public:
	void SetStream(const uint8_t *buffer) {
		this->buffer = buffer;
		index = 0;
	}

	size_t Index() const {
		return index;
	}

	template <class T>
	T ReadValue() {
		auto result = Load<T>(buffer + index);
		index += sizeof(T);
		return result;
	}

	template <class T, uint8_t SIZE>
	T ReadValue() {
		return ReadValue<T>(SIZE);
	}

	template <class T>
	inline T ReadValue(uint8_t bytes, uint8_t trailing_zero) {
		T result = 0;
		switch (bytes) {
		case 1:
			result = Load<uint8_t>(buffer + index);
			index++;
			return result;
		case 2:
			result = Load<uint16_t>(buffer + index);
			index += 2;
			return result;
		case 3:
			memcpy(&result, (void *)(buffer + index), 3);
			index += 3;
			return result;
		case 4:
			result = Load<uint32_t>(buffer + index);
			index += 4;
			return result;
		case 5:
			return MemcpyWrap<T>(
			    [&]() {
				    memcpy(&result, (void *)(buffer + index), 5);
				    index += 5;
				    return result;
			    },
			    5);
		case 6:
			return MemcpyWrap<T>(
			    [&]() {
				    memcpy(&result, (void *)(buffer + index), 6);
				    index += 6;
				    return result;
			    },
			    6);
		case 7:
			return MemcpyWrap<T>(
			    [&]() {
				    memcpy(&result, (void *)(buffer + index), 7);
				    index += 7;
				    return result;
			    },
			    7);
		default:
			return MemcpyWrap<T>(
			    [&]() {
				    if (sizeof(T) == 8 && trailing_zero < 8) {
					    result = Load<T>(buffer + index);
					    index += sizeof(T);
				    }
				    return result;
			    },
			    8);
		}
	}

private:
	template <class T>
	inline T MemcpyWrap(std::function<T(void)> func, idx_t size) {
		if (sizeof(T) > 4) {
			return func();
		} else {
			throw InternalException("Memcpy of size %d would exceed size %d of the return type", size, sizeof(T));
		}
	}

private:
	const uint8_t *buffer;
	uint32_t index;
};

} // namespace duckdb
