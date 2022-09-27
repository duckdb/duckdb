#pragma once

#include <stdint.h>

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

// Only the last group is potentially not 1024 (GROUP_SIZE) values in size
// But we can determine from the count of the segment whether this is the case or not
// So we can just read/write from left to right
class FlagBuffer {
public:
	FlagBuffer() : counter(0), buffer(nullptr), buffer_size(0) {}
public:
	void SetBuffer(uint8_t* buffer) {
		this->buffer = buffer;
	}
	void Insert(const uint8_t &value) {
		if ((counter & 3) == 0) {
			// Start the new byte fresh
			buffer[counter >> 2] = 0;
		}
		buffer[counter >> 2] |= (value & 3) << flag_shifts[counter & 3];
		counter++;
	}
	uint8_t Extract() {
		uint8_t result = (buffer[counter >> 2] & flag_masks[counter & 3]) >> flag_shifts[counter & 3];
		counter++;
		return result;
	}
private:
private:
	uint32_t counter = 0;
	uint8_t *buffer;
};

} //namespace duckdb_chimp
