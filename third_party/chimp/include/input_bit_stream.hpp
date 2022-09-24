//===----------------------------------------------------------------------===//
//                         DuckDB
//
// third_party/chimp/include/input_bit_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include "bit_utils.hpp"
#include <assert.h>

namespace duckdb_chimp {

//! Every byte read touches at most 2 bytes (1 if it's perfectly aligned)
//! Within a byte we need to mask off the bytes that we're interested in
//! And then we need to shift these to the start of the byte
//! I.E we want 4 bits, but our bit index is 3, our mask becomes:
//! 0111 1000
//! With a shift of 3

//! Align the masks to the right
static const uint8_t masks[] = {
	0b00000000,
	0b10000000,
	0b11000000,
	0b11100000,
	0b11110000,
	0b11111000,
	0b11111100,
	0b11111110,
	0b11111111,
	//! These later masks are for the cases where bit_index + SIZE exceeds 8
	0b11111110,
	0b11111100,
	0b11111000,
	0b11110000,
	0b11100000,
	0b11000000,
	0b10000000,
};

static const uint8_t remainder_masks[] = {
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0b10000000,
	0b11000000,
	0b11100000,
	0b11110000,
	0b11111000,
	0b11111100,
	0b11111110,
	0b11111111,
};

//! Left shifts
static const uint8_t shifts[] = {
	0, //unused
	7,
	6,
	5,
	4,
	3,
	2,
	1,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0
};

//! Right shifts
//! Right shift the values to cut off the mask when SIZE + bit_index exceeds 8
static const uint8_t right_shifts[] = {
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
};

struct InputBitStream {
public:
public:
	InputBitStream() : input(nullptr), bit_index(0), byte_index(0) {}
	uint8_t *input;
	uint8_t bit_index; //Index in the current byte, starting from the right
	uint64_t byte_index;
public:
	void SetStream(uint8_t* input) {
		this->input = input;
		bit_index = 0;
		byte_index = 0;
	}

	static inline uint8_t CreateMask(uint8_t size, uint8_t bit_index) {
		return (masks[size] >> bit_index);
	}

	inline uint8_t InnerRead(uint8_t size) {
		const uint8_t left_shift = 8 - size;
		// Create a mask given the size and bit_index
		uint8_t result = ((input[byte_index] & CreateMask(size, bit_index)) << bit_index) >> left_shift;
		byte_index += (size + bit_index >= 8);
		const uint8_t bit_remainder = (size + bit_index) - 8;
		result |= ((input[byte_index] & remainder_masks[size + bit_index]) >> (8 - bit_remainder));
		bit_index = (size + bit_index) & 7;
		return result;
	}

	template <class T, uint8_t SIZE>
	inline T ReadValue() {
		T result = 0;
		uint8_t iterations = SIZE >> 3; //divide by 8;
		while (iterations-- != 0) {
			result = result << 8 | InnerRead(8);
		}
		const uint8_t remainder = SIZE & 7;
		if (remainder) {
			result = result << remainder | InnerRead(remainder);
		}
		return result;
	}

	template <class T>
	inline T ReadValue(uint8_t size = sizeof(T) * __CHAR_BIT__) {
		T result = 0;
		uint8_t iterations = size >> 3; //divide by 8;
		while (iterations-- != 0) {
			result = result << 8 | InnerRead(8);
		}
		const uint8_t remainder = size & 7;
		if (remainder) {
			result = result << remainder | InnerRead(remainder);
		}
		return result;
	}
};

} //namespace duckdb_chimp
