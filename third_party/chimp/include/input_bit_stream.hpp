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
#include <exception>
#include <stdexcept>
#include <string>

namespace duckdb_chimp {

//! Every byte read touches at most 2 bytes (1 if it's perfectly aligned)
//! Within a byte we need to mask off the bits that we're interested in

//! Align the masks to the right
static constexpr uint8_t masks[] = {
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

	static inline uint8_t CreateMask(const uint8_t &size, const uint8_t &bit_index) {
		return (masks[size] >> bit_index);
	}

	inline uint8_t InnerReadByte() {
		uint8_t result = input[byte_index] << bit_index | ((input[byte_index + 1] & remainder_masks[8 + bit_index]) >> (8 - bit_index));
		byte_index++;
		return result;
	}

	//! bit_index: 4
	//! size: 7
	//! input: [12345678][12345678]
	//! result:   [-AAAA  BBB]
	//!
	//! Result contains 4 bits from the first byte (making up the most significant bits)
	//! And 3 bits from the second byte (the least significant bits)
	inline uint8_t InnerRead(const uint8_t &size) {
		const uint8_t right_shift = 8 - size;
		const uint8_t bit_remainder = (8 - ((size + bit_index) - 8)) & 7;
		// The least significant bits are positioned at the far right of the byte

		// Create a mask given the size and bit_index
		// Take the first byte
		// Left-shift it by bit_index, to line up the bits we're interested in with the mask
		// Get the mask for the given size
		// Bit-wise AND the byte and the mask together
		// Right-shift this result (the most significant bits)

		// Sometimes we will need to read from the second byte
		// But to make this branchless, we will perform what is basically a no-op if this condition is not true
		// SPILL = (bit_index + size >= 8)
		// 
		// If SPILL is true:
		// The remainder_masks gives us the mask for the bits we're interested in
		// We bit-wise AND these together (no need to shift anything because the bit_index is essentially zero for this new byte)
		// And we then right-shift these bits in place (to the right of the previous bits)
		const bool spill_to_next_byte = (size + bit_index >= 8);
		uint8_t result = ((input[byte_index] << bit_index) & masks[size]) >> right_shift | ((input[byte_index + spill_to_next_byte] & remainder_masks[size + bit_index]) >> bit_remainder);
		byte_index += spill_to_next_byte;
		bit_index = (size + bit_index) & 7;
		return result;
	}

    template <class T, uint8_t BYTES>
    inline T ReadBytes(const uint8_t &remainder) {
        T result = 0;
        for (uint8_t i = 0; i < BYTES; i++) {
            result = result << 8 | InnerReadByte();
        }
        return result << remainder | InnerRead(remainder);
    }

	template <class T, uint8_t SIZE>
	inline T ReadValue() {
		constexpr uint8_t BYTES = (SIZE >> 3);
		constexpr uint8_t REMAINDER = (SIZE & 7);
		return ReadBytes<T, BYTES>(REMAINDER);
	}

	template <class T>
	inline T ReadValue(uint8_t size = sizeof(T) * __CHAR_BIT__) {
		const uint8_t bytes = size >> 3; //divide by 8;
		const uint8_t remainder = size & 7;
		switch (bytes) {
			case 0: return ReadBytes<uint8_t, 0>(remainder);
			case 1: return ReadBytes<uint16_t, 1>(remainder);
			case 2: return ReadBytes<uint32_t, 2>(remainder);
			case 3: return ReadBytes<uint32_t, 3>(remainder);
			case 4: return ReadBytes<uint64_t, 4>(remainder);
			case 5: return ReadBytes<uint64_t, 5>(remainder);
			case 6: return ReadBytes<uint64_t, 6>(remainder);
			case 7: return ReadBytes<uint64_t, 7>(remainder);
			case 8: return ReadBytes<uint64_t, 8>(remainder);
			default: throw std::runtime_error("ReadValue reports that it needs to read " + std::to_string(bytes) + " bytes");
		}
	}
};

} //namespace duckdb_chimp
