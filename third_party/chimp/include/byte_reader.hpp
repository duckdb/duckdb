#pragma once

#include <stdint.h>
#include <iostream>
#include "duckdb/common/fast_mem.hpp"

namespace duckdb_chimp {

// This class reads arbitrary amounts of bits from a buffer
// If 41 bits are requested (5 bytes + 1 bit), we will read 6 bytes and increment the byte index by 6
// With the assumption that the remainder of the last byte read is zero-initialized
class ByteReader {
private:
	static constexpr uint8_t final_shifts[8] = {
		0,
		7,
		6,
		5,
		4,
		3,
		2,
		1
	};
public:
	ByteReader() : buffer(nullptr), index(0) {

	}
public:
	void SetStream(uint8_t* buffer) {
		this->buffer = buffer;
		index = 0;
	}

	template <class T>
	T ReadValue() {
		throw std::runtime_error("fixme read value");
//		uint64_t bytes = 0;
//		const uint8_t bytes_to_read = (SIZE >> 3) + ((SIZE & 7) != 0);
//		duckdb::FastMemcpy(&bytes, (void*)(buffer + index), bytes_to_read);
//		T result = (T)bytes;
//		index += bytes_to_read;
//		return result;
	}

	template <>
	uint8_t ReadValue<uint8_t>() {
		auto result = duckdb::Load<uint8_t>(buffer + index);
		index ++;
		return result;
	}
	template <>
	uint16_t ReadValue<uint16_t>() {
		auto result = duckdb::Load<uint16_t>(buffer + index);
		index += 2;
		return result;
	}
	template <>
	uint32_t ReadValue<uint32_t>() {
		auto result = duckdb::Load<uint32_t>(buffer + index);
		index += 4;
		return result;
	}
	template <>
	uint64_t ReadValue<uint64_t>() {
		auto result = duckdb::Load<uint64_t>(buffer + index);
		index += 8;
		return result;
	}

	template <class T> 
	T ReadValue(const uint8_t &size) {
		T result = 0;
		switch(size) {
		case 1:
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
		case 8:
			result = duckdb::Load<uint8_t>(buffer + index);
			index++;
			return result;
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
		case 16:
			result = duckdb::Load<uint16_t>(buffer + index);
			index+=2;
			return result;
		case 17:
		case 18:
		case 19:
		case 20:
		case 21:
		case 22:
		case 23:
		case 24:
			memcpy(&result, (void *)(buffer + index), 3);
			index+=3;
			return result;
		case 25:
		case 26:
		case 27:
		case 28:
		case 29:
		case 30:
		case 31:
		case 32:
			result = duckdb::Load<uint32_t>(buffer + index);
			index+=4;
			return result;
		case 33:
		case 34:
		case 35:
		case 36:
		case 37:
		case 38:
		case 39:
		case 40:
			memcpy(&result, (void *)(buffer + index), 5);
			index+=5;
			return result;
		case 41:
		case 42:
		case 43:
		case 44:
		case 45:
		case 46:
		case 47:
		case 48:
			memcpy(&result, (void *)(buffer + index), 6);
			index+=6;
			return result;
		case 49:
		case 50:
		case 51:
		case 52:
		case 53:
		case 54:
		case 55:
		case 56:
			memcpy(&result, (void *)(buffer + index), 7);
			index+=7;
			return result;
		default:
			result = duckdb::Load<uint64_t>(buffer + index);
//			memcpy(&result, (void *)(buffer + index), 8);
			index+=8;
			return result;
		}
	}
private:
private:
	uint8_t *buffer;
	uint32_t index;
};

} //namespace duckdb
