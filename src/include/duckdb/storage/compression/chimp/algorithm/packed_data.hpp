//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/packed_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp"
#include "duckdb.h"

namespace duckdb {

struct UnpackedData {
	uint8_t leading_zero;
	uint8_t significant_bits;
	uint8_t index;
};

template <class CHIMP_TYPE>
struct PackedDataUtils {
private:
	static constexpr uint8_t INDEX_BITS_SIZE = 7;
	static constexpr uint8_t LEADING_BITS_SIZE = 3;

	static constexpr uint8_t INDEX_MASK = ((uint8_t)1 << INDEX_BITS_SIZE) - 1;
	static constexpr uint8_t LEADING_MASK = ((uint8_t)1 << LEADING_BITS_SIZE) - 1;

	static constexpr uint8_t INDEX_SHIFT_AMOUNT = (sizeof(uint16_t) * 8) - INDEX_BITS_SIZE;
	static constexpr uint8_t LEADING_SHIFT_AMOUNT = INDEX_SHIFT_AMOUNT - LEADING_BITS_SIZE;

public:
	//|----------------|	//! INITIAL_FILL(16) bits
	// IIIIIII				//! Index (7 bits, shifted by 9)
	//        LLL			//! LeadingZeros (3 bits, shifted by 6)
	//           SSSSSS 	//! SignificantBits (6 bits)
	static inline void Unpack(uint16_t packed_data, UnpackedData &dest) {
		dest.index = packed_data >> INDEX_SHIFT_AMOUNT & INDEX_MASK;
		dest.leading_zero = packed_data >> LEADING_SHIFT_AMOUNT & LEADING_MASK;
		dest.leading_zero = ChimpConstants::Decompression::LEADING_REPRESENTATION[dest.leading_zero];
		dest.significant_bits = packed_data & SignificantBits<CHIMP_TYPE>::mask;
		if (dest.significant_bits == 0) {
			dest.significant_bits = 64;
		}
		// Verify that combined, this is not bigger than the full size of the type
		D_ASSERT(dest.significant_bits + dest.leading_zero <= (sizeof(CHIMP_TYPE) * 8));
	}
	static inline uint16_t Pack(uint8_t index, uint8_t leading_zero, uint8_t significant_bits) {
		static constexpr uint8_t BIT_SIZE = (sizeof(CHIMP_TYPE) * 8);

		// Verify that combined (with significant bits set to full size of the type if it's 0), this is not bigger than
		// the full size of the type;
		D_ASSERT(leading_zero + ((!significant_bits) * (sizeof(CHIMP_TYPE) * 8)) + significant_bits <=
		         (sizeof(CHIMP_TYPE) * 8));
		uint16_t result = 0;
		result += ((uint32_t)BIT_SIZE * 8) * (ChimpConstants::BUFFER_SIZE + index);
		result += BIT_SIZE * ChimpConstants::Compression::LEADING_REPRESENTATION[leading_zero];
		if (BIT_SIZE == 32) {
			// Shift the result by 1 to occupy the 16th bit
			result <<= 1;
		}
		result += significant_bits;

		return result;
	}
};

template <bool EMPTY>
struct PackedDataBuffer {
public:
	PackedDataBuffer() : index(0), buffer(nullptr) {
	}

public:
	void SetBuffer(uint16_t *buffer) {
		this->buffer = buffer;
		this->index = 0;
	}

	void Reset() {
		this->index = 0;
	}

	inline void Insert(uint16_t packed_data) {
		if (!EMPTY) {
			buffer[index] = packed_data;
		}
		index++;
	}

	idx_t index;
	uint16_t *buffer;
};

} // namespace duckdb
