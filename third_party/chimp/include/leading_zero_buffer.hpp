#pragma once

#include <stdint.h>
#include <cstring>
#ifdef DEBUG
#include <vector>
#endif

namespace duckdb_chimp {

#define BLOCK_IDX ((counter >> 3) * (LEADING_ZERO_BLOCK_BIT_SIZE / 8))

//! This class is in charge of storing the leading_zero_bits, which are of a fixed size
//! These are packed together so that the rest of the data can be byte-aligned
//! The leading zero bit data is read from left to right
template <bool EMPTY>
class LeadingZeroBuffer {
public:
static constexpr uint32_t CHIMP_GROUP_SIZE = 1024;
static constexpr uint32_t LEADING_ZERO_BITS_SIZE = 3;
static constexpr uint32_t LEADING_ZERO_BLOCK_SIZE = 8;
static constexpr uint32_t LEADING_ZERO_BLOCK_BIT_SIZE = LEADING_ZERO_BLOCK_SIZE * LEADING_ZERO_BITS_SIZE;
static constexpr uint32_t MAX_LEADING_ZERO_BLOCKS = CHIMP_GROUP_SIZE / LEADING_ZERO_BLOCK_SIZE;
static constexpr uint32_t MAX_BITS_USED_BY_ZERO_BLOCKS = MAX_LEADING_ZERO_BLOCKS * LEADING_ZERO_BLOCK_BIT_SIZE;
static constexpr uint32_t MAX_BYTES_USED_BY_ZERO_BLOCKS = MAX_BITS_USED_BY_ZERO_BLOCKS / 8;

// Add an extra byte to prevent heap buffer overflow on the last group, because we'll be addressing 4 bytes each
static constexpr uint32_t BUFFER_SIZE = MAX_BYTES_USED_BY_ZERO_BLOCKS + (sizeof(uint32_t) - (LEADING_ZERO_BLOCK_BIT_SIZE / 8));

//static constexpr uint32_t leading_zero_masks[8] = {
//	0xE0000000, //0b 11100000 00000000 00000000 00000000,
//	0x1C000000, //0b 00011100 00000000 00000000 00000000,
//	0x03800000, //0b 00000011 10000000 00000000 00000000,
//	0x00700000, //0b 00000000 01110000 00000000 00000000,
//	0x000E0000, //0b 00000000 00001110 00000000 00000000,
//	0x0001C000, //0b 00000000 00000001 11000000 00000000,
//	0x00003800, //0b 00000000 00000000 00111000 00000000,
//	0x00000700, //0b 00000000 00000000 00000111 00000000,
//};
static constexpr uint32_t leading_zero_masks[8] = {
	7,        // 0b 00000000 00000000 00000000 00000111,
	56,       // 0b 00000000 00000000 00000000 00111000,
	448,      // 0b 00000000 00000000 00000001 11000000,
	3584,     // 0b 00000000 00000000 00001110 00000000,
	28672,    // 0b 00000000 00000000 01110000 00000000,
	229376,   // 0b 00000000 00000011 10000000 00000000,
	1835008,  // 0b 00000000 00011100 00000000 00000000,
	14680064, // 0b 00000000 11100000 00000000 00000000,
};

// We're not using the last byte (the most significant) of the 4 bytes we're accessing
static constexpr uint8_t leading_zero_shifts[8] = {
	0,
	3,
	6,
	9,
	12,
	15,
	18,
	21
};

public:
	LeadingZeroBuffer() : current(0), counter(0), buffer(nullptr) {}
	void SetBuffer(uint8_t* buffer) {
		// Set the internal buffer, when inserting this should be BUFFER_SIZE bytes in length
		// This buffer does not need to be zero-initialized for inserting
		this->buffer = buffer;
		this->counter = 0;
	}
	void Flush() {
		//THIS NEEDS TO FLUSH THE 'CURRENT' TO THE BLOCK... DUHHHH
		if ((counter & 7) != 0) {
			FlushBuffer();
		}
	}

	//Reset the counter, but don't replace the buffer
	void Reset() {
		this->counter = 0;
		current = 0;
		//if (!EMPTY && buffer) {
		//	buffer[0] = 0;
		//	buffer[1] = 0;
		//	buffer[2] = 0;
		//}
		#ifdef DEBUG
			flags.clear();
		#endif
	}
public:
	#ifdef DEBUG
		uint8_t ExtractValue(uint32_t value, uint8_t index) {
			return (value & leading_zero_masks[index]) >> leading_zero_shifts[index];
		}
	#endif

	void FlushBuffer() {
		if (EMPTY) {
			return;
		}
		const auto buffer_idx = BLOCK_IDX;
		std::memcpy((void*)(buffer + buffer_idx), (uint8_t*)&current, 3);
		#ifdef DEBUG
			// Verify that the bits are copied correctly

			uint32_t temp_value = 0;
			std::memcpy((uint8_t*)&temp_value, (void*)(buffer + buffer_idx), 3);
			for (size_t i = 0; i < flags.size(); i++) {
				assert(flags[i] == ExtractValue(temp_value, i));
			}
			flags.clear();
		#endif
	}

	void Insert(const uint8_t &value) {
		if (!EMPTY) {
			#ifdef DEBUG
				flags.push_back(value);
			#endif
			current |= (value & 7) << leading_zero_shifts[counter & 7];
			#ifdef DEBUG
				//Verify that the bits are serialized correctly
				assert(flags[counter & 7] == ExtractValue(current, counter & 7));
			#endif

			if ((counter & (LEADING_ZERO_BLOCK_SIZE-1)) == 7) {
				FlushBuffer();
				current = 0;
			}
		}
		counter++;
	}
	uint8_t Extract() {
		const auto buffer_idx = BLOCK_IDX;
		uint32_t temp;
		//TODO: only copy the bytes relevant to the current value (something % 3) ..
		std::memcpy((uint8_t*)&temp, (void*)(buffer + buffer_idx), 3);

		uint8_t result = (temp & leading_zero_masks[counter & 7]) >> leading_zero_shifts[counter & 7];
		counter++;
		return result;
	}
	size_t BlockCount() const {
		return (counter >> 3) + ((counter & 7) != 0);
	}
private:
private:
	uint32_t current;
	uint32_t counter = 0; //block_index * 8
	uint8_t *buffer;
	#ifdef DEBUG
	std::vector<uint8_t> flags;
	#endif
};

} //namespace duckdb_chimp
