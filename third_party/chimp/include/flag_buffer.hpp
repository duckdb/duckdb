#pragma once

#include <stdint.h>
#include <iostream>

namespace duckdb_chimp {

static constexpr uint8_t flag_masks[4] = {
	192, // 0b1100 0000,
	48,  // 0b0011 0000,
	12,  // 0b0000 1100,
	3,   // 0b0000 0011,
};

static constexpr uint8_t flag_shifts[4] = {
	6,
	4,
	2,
	0
};

// This class is responsible for writing and reading the flag bits
// Only the last group is potentially not 1024 (GROUP_SIZE) values in size
// But we can determine from the count of the segment whether this is the case or not
// So we can just read/write from left to right
template <bool EMPTY>
class FlagBuffer {
public:
	FlagBuffer() : counter(0), buffer(nullptr) {}
public:
	void SetBuffer(uint8_t* buffer) {
		this->buffer = buffer;
		this->counter = 0;
	}
	void Reset() {
		this->counter = 0;
	}
	void Insert(const uint8_t &value) {
		std::cout << "[FLAG] WRITE: " << (uint64_t)value << std::endl;
		if (!EMPTY) {
			if ((counter & 3) == 0) {
				// Start the new byte fresh
				buffer[counter >> 2] = 0;
			}
			buffer[counter >> 2] |= ((value & 3) << flag_shifts[counter & 3]);
		}
		counter++;
	}
	uint8_t Extract() {
		uint8_t result = (buffer[counter >> 2] & flag_masks[counter & 3]) >> flag_shifts[counter & 3];
		counter++;
		std::cout << "[FLAG] READ: " << (uint64_t)result << std::endl;
		return result;
	}
	uint32_t BytesUsed() const {
		return (counter >> 2) + ((counter & 3) != 0);
	}
private:
private:
	uint32_t counter = 0;
	uint8_t *buffer;
};

} //namespace duckdb_chimp
