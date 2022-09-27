#pragma once

#include <stdint.h>
#include <cstring>

namespace duckdb_chimp {

#define BLOCK_IDX ((counter >> 3) * (LEADING_ZERO_BLOCK_BIT_SIZE / 8))

//! This class is in charge of storing the leading_zero_bits, which are of a fixed size
//! These are packed together so that the rest of the data can be byte-aligned
//! This data is read from left to right
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

static constexpr uint32_t leading_zero_masks[8] = {
	0xE0000000, //0b 11100000 00000000 00000000 00000000,
	0x1C000000, //0b 00011100 00000000 00000000 00000000,
	0x03800000, //0b 00000011 10000000 00000000 00000000,
	0x00700000, //0b 00000000 01110000 00000000 00000000,
	0x000E0000, //0b 00000000 00001110 00000000 00000000,
	0x0001C000, //0b 00000000 00000001 11000000 00000000,
	0x00003800, //0b 00000000 00000000 00111000 00000000,
	0x00000700, //0b 00000000 00000000 00000111 00000000,
};

// We're not using the last byte of the 4 bytes we're accessing
static constexpr leading_zero_shifts[8] = {
	29,
	26,
	23,
	20,
	17
	14,
	11,
	8
};

public:
	LeadingZeroBuffer() : counter(0), buffer(nullptr) {}
	void SetBuffer(uint8_t* buffer) {
		// Set the internal buffer, when inserting this should be BUFFER_SIZE bytes in length
		// This buffer does not need to be zero-initialized for inserting
		this->buffer = buffer;
	}
public:
	void Insert(const uint8_t &value) {
		const auto buffer_idx = BLOCK_IDX;
		if ((counter & (LEADING_ZERO_BLOCK_SIZE-1)) == 0) {
			//Start fresh block
			*((uint32_t*)(buffer + buffer_idx)) = 0;
		}
		*((uint32_t*)(buffer + buffer_idx)) |= (value & 7) << leading_zero_shifts[counter & 7];
		counter++;
	}
	uint8_t Extract() {
		const auto buffer_idx = BLOCK_IDX;
		uint8_t result = (*((uint32_t*)(buffer + buffer_idx)) & leading_zero_masks[counter & 7]) >> leading_zero_shifts[counter & 7];
		counter++;
		return result;
	}
private:
private:
	uint32_t counter = 0; //block_index * 8
	uint8_t *buffer;
};

} //namespace duckdb_chimp
