#pragma once

#include <stdint.h>
#include <iostream>

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

	uint8_t ReadByte(const uint32_t& offset, const uint8_t& bytes_to_read) const {
		// Dont touch bytes that we shouldn't
		// If offset is too high, return the result * 0
		return buffer[index + ((offset+1 >= bytes_to_read) * offset)] * (offset+1 >= bytes_to_read);
	}

	void PrintResult(uint64_t result, const uint8_t &size) {
		static thread_local uint64_t counter = 0;
		//std::cout << "READ[" << counter++ << "]: " << (uint64_t)result << " | SIZE: " << (uint64_t)size << std::endl;
		//std::cout << "R" << counter++ << ": " << (uint64_t)result << std::endl;
	}

	template <class T, uint8_t SIZE>
	T ReadValue() {
		uint64_t bytes = 0;
		const uint8_t bytes_to_read = (SIZE >> 3) + ((SIZE & 7) != 0);
		std::memcpy(&bytes, (void*)(buffer + index), bytes_to_read);
		T result = (T)bytes;
		index += bytes_to_read;
		PrintResult(result, SIZE);
		return result;
	}

	template <class T> 
	T ReadValue(const uint8_t &size) {
		uint64_t bytes = 0;
		const uint8_t bytes_to_read = (size >> 3) + ((size & 7) != 0);
		//bytes[0] = ReadByte(0, bytes_to_read);
		//bytes[1] = ReadByte(1, bytes_to_read);
		//bytes[2] = ReadByte(2, bytes_to_read);
		//bytes[3] = ReadByte(3, bytes_to_read);
		//bytes[4] = ReadByte(4, bytes_to_read);
		//bytes[5] = ReadByte(5, bytes_to_read);
		//bytes[6] = ReadByte(6, bytes_to_read);
		//bytes[7] = ReadByte(7, bytes_to_read);
		//index += bytes_to_read;
		//// Bytes are packed most-significant first, so if we're only interested in 2 bits, we need to shift them 6 to the right
		////auto result = (T)(*((uint64_t*)(bytes)) >> final_shifts[size & 7]);
		//std::cout << "READ: " << (uint64_t)result << " | SIZE: " << (uint64_t)size << std::endl;
		std::memcpy(&bytes, (void*)(buffer + index), bytes_to_read);
		T result = (T)bytes;
		index += bytes_to_read;
		//result = result >> final_shifts[(size & 7)];
		PrintResult(result, size);
		return result;
	}
private:
private:
	uint8_t *buffer;
	uint32_t index;
};

} //namespace duckdb
