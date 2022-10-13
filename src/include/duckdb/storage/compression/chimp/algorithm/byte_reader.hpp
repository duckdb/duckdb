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
		throw InternalException("Specialization for ReadValue is not implemented");
	}

	template <>
	uint8_t ReadValue<uint8_t>() {
		auto result = Load<uint8_t>(buffer + index);
		index++;
		return result;
	}
	template <>
	uint16_t ReadValue<uint16_t>() {
		auto result = Load<uint16_t>(buffer + index);
		index += 2;
		return result;
	}
	template <>
	uint32_t ReadValue<uint32_t>() {
		auto result = Load<uint32_t>(buffer + index);
		index += 4;
		return result;
	}
	template <>
	uint64_t ReadValue<uint64_t>() {
		auto result = Load<uint64_t>(buffer + index);
		index += 8;
		return result;
	}

	template <class T, uint8_t SIZE>
	T ReadValue() {
		return ReadValue<T>(SIZE);
	}

	template <class T>
	inline T ReadValue(uint8_t bytes) {
		T result = 0;
		switch (bytes) {
		case 1:
			result = duckdb::Load<uint8_t>(buffer + index);
			index++;
			return result;
		case 2:
			result = duckdb::Load<uint16_t>(buffer + index);
			index += 2;
			return result;
		case 3:
			memcpy(&result, (void *)(buffer + index), 3);
			index += 3;
			return result;
		case 4:
			result = duckdb::Load<uint32_t>(buffer + index);
			index += 4;
			return result;
		case 5:
			memcpy(&result, (void *)(buffer + index), 5);
			index += 5;
			return result;
		case 6:
			memcpy(&result, (void *)(buffer + index), 6);
			index += 6;
			return result;
		case 7:
			memcpy(&result, (void *)(buffer + index), 7);
			index += 7;
			return result;
		default:
			result = duckdb::Load<T>(buffer + index);
			index += sizeof(T);
			return result;
		}
	}

private:
	const uint8_t *buffer;
	uint32_t index;
};

} // namespace duckdb
