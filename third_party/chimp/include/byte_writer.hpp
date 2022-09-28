#pragma once

#include <stdint.h>
#include <cstring>
#include <iostream>

namespace duckdb_chimp {

template <bool EMPTY>
class ByteWriter {
public:
	ByteWriter() : buffer(nullptr), index(0) {}
public:
	size_t BytesWritten() const {
		return index;
	}

	void Flush() {
	}

	void ByteAlign() {
	}

	void SetStream(uint8_t *buffer) {
		this->buffer = buffer;
		this->index = 0;
	}

	template <class T, uint8_t SIZE>
	void WriteValue(const T &value) {
		const uint8_t bytes = (SIZE >> 3) + ((SIZE & 7) != 0);
		if (!EMPTY) {
			std::memcpy((void*)(buffer + index), &value, bytes);
		}
		index += bytes;
	}

	template <class T>
	void WriteValue(const T &value, const uint8_t &size) {
		const uint8_t bytes = (size >> 3) + ((size & 7) != 0);
		if (!EMPTY) {
			std::memcpy((void*)(buffer + index), &value, bytes);
		}
		index += bytes;
	}
private:
private:
	uint8_t *buffer;
	size_t index;
};

} //namespace duckdb_chimp
