//===----------------------------------------------------------------------===//
//                         DuckDB
//
// third_party/chimp/include/bit_reader.hpp
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

#define BYTE_INDEX ((bit_index) >> 3)
#define BIT_INDEX ((bit_index) & 7)

//! Align the masks to the right
static constexpr uint8_t masks[] = {
	0,   //0b00000000,
	128, //0b10000000,
	192, //0b11000000,
	224, //0b11100000,
	240, //0b11110000,
	248, //0b11111000,
	252, //0b11111100,
	254, //0b11111110,
	255, //0b11111111,
	// These later masks are for the cases where bit_index + SIZE exceeds 8
	254, //0b11111110,
	252, //0b11111100,
	248, //0b11111000,
	240, //0b11110000,
	224, //0b11100000,
	192, //0b11000000,
	128, //0b10000000,
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
	128, //0b10000000,
	192, //0b11000000,
	224, //0b11100000,
	240, //0b11110000,
	248, //0b11111000,
	252, //0b11111100,
	254, //0b11111110,
	255, //0b11111111,
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

struct BitReader {
public:
public:
	BitReader() : input(nullptr), bit_index(0) {}
	uint8_t *input;
	uint32_t bit_index;
public:
	void SetStream(uint8_t* input) {
		this->input = input;
		bit_index = 0;
	}

	inline uint8_t InnerReadByte(const uint8_t& offset) {
		uint8_t result = input[BYTE_INDEX + offset] << BIT_INDEX | ((input[BYTE_INDEX + offset + 1] & remainder_masks[8 + BIT_INDEX]) >> (8 - BIT_INDEX));
		return result;
	}

	//template <uint8_t BIT_INDEX, uint8_t REMAINDER>
	//inline uint8_t InnerReadTemplated() {
	//	const uint8_t right_shift = 8 - size;
	//	const uint8_t bit_remainder = (8 - ((size + bit_index) - 8)) & 7;

	//	if (BIT_INDEX) {
	//		// Need to apply bit-shifts

	//	}
	//	else {
	//		//
	//	}
	//	const bool spill_to_next_byte = (size + bit_index >= 8);
	//	uint8_t result = ((input[byte_index] << bit_index) & masks[size]) >> right_shift | ((input[byte_index + spill_to_next_byte] & remainder_masks[size + bit_index]) >> bit_remainder);
	//	byte_index += spill_to_next_byte;
	//	bit_index = (size + bit_index) & 7;
	//	return result;
	//}

	//! bit_index: 4
	//! size: 7
	//! input: [12345678][12345678]
	//! result:   [-AAAA  BBB]
	//!
	//! Result contains 4 bits from the first byte (making up the most significant bits)
	//! And 3 bits from the second byte (the least significant bits)
	inline uint8_t InnerRead(const uint8_t &size, const uint8_t &offset) {
		const uint8_t right_shift = 8 - size;
		const uint8_t bit_remainder = (8 - ((size + BIT_INDEX) - 8)) & 7;
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
		const bool spill_to_next_byte = (size + BIT_INDEX >= 8);
		uint8_t result = ((input[BYTE_INDEX + offset] << BIT_INDEX) & masks[size]) >> right_shift | ((input[BYTE_INDEX + offset + spill_to_next_byte] & remainder_masks[size + BIT_INDEX]) >> bit_remainder);
		return result;
	}

	//template <uint8_t SIZE, uint8_t BIT_INDEX>
	//inline uint8_t InnerReadTemplatedInner(const uint8_t& offset) {
	//	constexpr uint8_t right_shift = 8 - SIZE;
	//	constexpr uint8_t bit_remainder = (8 - ((SIZE + BIT_INDEX) - 8)) & 7;

	//	constexpr bool spill_to_next_byte = (SIZE + BIT_INDEX >= 8);
	//	uint8_t result = ((input[byte_index + offset] << BIT_INDEX) & masks[SIZE]) >> right_shift | ((input[byte_index + offset + spill_to_next_byte] & remainder_masks[SIZE + BIT_INDEX]) >> bit_remainder);
	//	byte_index += spill_to_next_byte;
	//	bit_index = (SIZE + BIT_INDEX) & 7;
	//	return result;
	//}

	//template<uint8_t REMAINING>
	//inline uint8_t InnerReadTemplated(const uint8_t& offset) {
	//	switch(bit_index) {
	//	case 0: return InnerReadTemplatedInner<REMAINING, 0>(offset);
	//	case 1: return InnerReadTemplatedInner<REMAINING, 1>(offset);
	//	case 2: return InnerReadTemplatedInner<REMAINING, 2>(offset);
	//	case 3: return InnerReadTemplatedInner<REMAINING, 3>(offset);
	//	case 4: return InnerReadTemplatedInner<REMAINING, 4>(offset);
	//	case 5: return InnerReadTemplatedInner<REMAINING, 5>(offset);
	//	case 6: return InnerReadTemplatedInner<REMAINING, 6>(offset);
	//	case 7: return InnerReadTemplatedInner<REMAINING, 7>(offset);
	//	default: throw std::runtime_error("InnerReadTemplate not implemented for offset: " + std::to_string(offset));
	//	};
	//}

	//inline uint8_t InnerReadSwitch(const uint8_t &remaining, const uint8_t& offset) {
	//	switch(remaining) {
	//	case 0: return 0;
	//	case 1: return InnerReadTemplated<1>(offset);
	//	case 2: return InnerReadTemplated<2>(offset);
	//	case 3: return InnerReadTemplated<3>(offset);
	//	case 4: return InnerReadTemplated<4>(offset);
	//	case 5: return InnerReadTemplated<5>(offset);
	//	case 6: return InnerReadTemplated<6>(offset);
	//	case 7: return InnerReadTemplated<7>(offset);
	//	default: throw std::runtime_error("InnerReadSwitch not implemented for remaining: " + std::to_string(remaining));
	//	};
	//}

    template <class T, uint8_t BYTES>
    inline T ReadBytes(const uint8_t &remainder) {
        T result = 0;
        for (uint8_t i = 0; i < BYTES; i++) {
            result = result << 8 | InnerReadByte(i);
        }
        result = result << remainder | InnerRead(remainder, BYTES);
		bit_index += (BYTES << 3) + remainder;
        return result;
    }

    template <class T>
    inline T ReadBytes(const uint8_t &bytes, const uint8_t &remainder) {
        T result = 0;
        for (uint8_t i = 0; i < bytes; i++) {
            result = result << 8 | InnerReadByte(i);
        }
        result = result << remainder | InnerRead(remainder, bytes);
		bit_index += (bytes << 3) + remainder;
		return result;
    }

	template <class T, uint8_t SIZE>
	inline T ReadValue() {
		constexpr uint8_t BYTES = (SIZE >> 3);
		constexpr uint8_t REMAINDER = (SIZE & 7);
		return ReadBytes<T, BYTES>(REMAINDER);
	}

	template <class T>
	inline T ReadValue(const uint8_t &size) {
		const uint8_t bytes = size >> 3; //divide by 8;
		const uint8_t remainder = size & 7;
		return ReadBytes<T>(bytes, remainder);
	}
};

} //namespace duckdb_chimp
