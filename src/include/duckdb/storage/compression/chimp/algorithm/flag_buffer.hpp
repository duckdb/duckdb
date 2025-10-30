//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/flag_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp"
#ifdef DEBUG
#include "duckdb/common/vector.hpp"
#include "duckdb/common/assert.hpp"
#endif

namespace duckdb {

struct FlagBufferConstants {
	static constexpr uint8_t MASKS[4] = {
	    192, // 0b1100 0000,
	    48,  // 0b0011 0000,
	    12,  // 0b0000 1100,
	    3,   // 0b0000 0011,
	};

	static constexpr uint8_t SHIFTS[4] = {6, 4, 2, 0};
};

// This class is responsible for writing and reading the flag bits
// Only the last group is potentially not 1024 (GROUP_SIZE) values in size
// But we can determine from the count of the segment whether this is the case or not
// So we can just read/write from left to right
template <bool EMPTY>
class FlagBuffer {
public:
	FlagBuffer() : counter(0), buffer(nullptr) {
	}

public:
	void SetBuffer(uint8_t *buffer) {
		this->buffer = buffer;
		this->counter = 0;
	}
	void Reset() {
		this->counter = 0;
#ifdef DEBUG
		this->flags.clear();
#endif
	}

#ifdef DEBUG
	uint8_t ExtractValue(uint32_t value, uint8_t index) {
		return (value & FlagBufferConstants::MASKS[index]) >> FlagBufferConstants::SHIFTS[index];
	}
#endif

	uint64_t BitsWritten() const {
		return counter * 2ULL;
	}

	void Insert(ChimpConstants::Flags value) {
		if (!EMPTY) {
			if ((counter & 3) == 0) {
				// Start the new byte fresh
				buffer[counter >> 2] = 0;
#ifdef DEBUG
				flags.clear();
#endif
			}
#ifdef DEBUG
			flags.push_back((uint8_t)value);
#endif
			buffer[counter >> 2] |= (((uint8_t)value & 3) << FlagBufferConstants::SHIFTS[counter & 3]);
#ifdef DEBUG
			// Verify that the bits are serialized correctly
			D_ASSERT(flags[counter & 3] == ExtractValue(buffer[counter >> 2], counter & 3));
#endif
		}
		counter++;
	}
	inline uint8_t Extract() {
		const uint8_t result = (buffer[counter >> 2] & FlagBufferConstants::MASKS[counter & 3]) >>
		                       FlagBufferConstants::SHIFTS[counter & 3];
		counter++;
		return result;
	}

	uint32_t BytesUsed() const {
		return (counter >> 2) + ((counter & 3) != 0);
	}

	uint32_t FlagCount() const {
		return counter;
	}

private:
private:
	uint32_t counter = 0;
	uint8_t *buffer;
#ifdef DEBUG
	vector<uint8_t> flags;
#endif
};

} // namespace duckdb
