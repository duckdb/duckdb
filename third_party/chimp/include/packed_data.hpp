#pragma once

namespace duckdb_chimp {


static constexpr uint8_t INDEX_BITS_SIZE = 7;
static constexpr uint8_t LEADING_BITS_SIZE = 3;
static constexpr uint8_t SIGNIFICANT_BITS_SIZE = 6;

static constexpr uint8_t INITIAL_FILL = INDEX_BITS_SIZE + LEADING_BITS_SIZE + SIGNIFICANT_BITS_SIZE;

static constexpr uint8_t INDEX_MASK = ((uint8_t)1 << INDEX_BITS_SIZE) - 1;
static constexpr uint8_t LEADING_MASK = ((uint8_t)1 << LEADING_BITS_SIZE) - 1;
static constexpr uint8_t SIGNIFICANT_MASK = ((uint8_t)1 << SIGNIFICANT_BITS_SIZE) - 1;

static constexpr uint8_t INDEX_SHIFT_AMOUNT = INITIAL_FILL - INDEX_BITS_SIZE;
static constexpr uint8_t LEADING_SHIFT_AMOUNT = INDEX_SHIFT_AMOUNT - LEADING_BITS_SIZE;

struct UnpackedData {
	uint8_t leading_zero;
	uint8_t significant_bits;
	uint8_t index;
};

struct PackedDataUtils {
public:
	//|----------------|	//! INITIAL_FILL(16) bits
	// IIIIIII				//! Index (7 bits, shifted by 9)
	//        LLL			//! LeadingZeros (3 bits, shifted by 6)
	//           SSSSSS 	//! SignificantBits (6 bits)
	static inline void Unpack(uint16_t packed_data, UnpackedData& dest) {
		dest.index = packed_data >> INDEX_SHIFT_AMOUNT & INDEX_MASK;
		dest.leading_zero = packed_data >> LEADING_SHIFT_AMOUNT & LEADING_MASK;
		dest.significant_bits = packed_data & SIGNIFICANT_MASK;
		if (dest.significant_bits == 0) {
			dest.significant_bits = 64;
		}
	}
};

#include <stdint.h>

template <bool EMPTY>
struct PackedDataBuffer {
public:
	PackedDataBuffer() : index(0), buffer(nullptr) {}
public:
	void SetBuffer(uint16_t* buffer) {
		this->buffer = buffer;
		this->index = 0;
	}

	void Reset() {
		this->index = 0;
	}

	inline void Insert(uint16_t packed_data) {
		if (EMPTY) {
			return;
		}
		buffer[index++] = packed_data;
	}

	size_t		index;
	uint16_t*	buffer;
};

} //namespace duckdb_chimp
