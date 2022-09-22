#pragma once

#include <stdint.h>
#include "bit_utils.hpp"
#include <assert.h>

namespace duckdb_chimp {

static constexpr uint32_t BLOCK_SIZE = 262136;

//! Set this to uint64_t, not sure what setting a double to 0 does on the bit-level
class InputBitStream {
public:
	using INTERNAL_TYPE = uint8_t;

	InputBitStream() :
		stream(nullptr),
		current(0),
		fill(0),
		stream_index(0)
		{
		}
public:
	static constexpr uint8_t INTERNAL_TYPE_BITSIZE = sizeof(INTERNAL_TYPE) * 8;

	void SetStream(uint8_t* input_stream) {
		stream = input_stream;
		stream_index = 0;
		current = 0;
		fill = 0;
		bits_read = 0;
		Refill();
		Refill();
	}

	//! The amount of bytes we've read from the stream (ceiling)
	size_t ByteSize() const {
		return (stream_index * sizeof(INTERNAL_TYPE)) + 1;
	}
	//! The amount of bits we've read from the stream
	size_t BitSize() const {
		return (stream_index * INTERNAL_TYPE_BITSIZE) + fill;
	}

	template <class T>
	T ReadValue(uint8_t value_size = (sizeof(T) * 8)) {
		int32_t i;
		T value = 0;

		if (LoadedEnough(value_size)) {
			// Can directly read from current
			return (T)ReadFromCurrent(value_size);
		}

		value_size -= fill;
		// Empty the current bit buffer
		value = (T)ReadFromCurrent(fill);

		// Read multiples of 8
		i = value_size >> 4;
		while(i-- != 0) {
			value = value << 16 | (T)ReadFromCurrent(16);
		}

		// Get the last (< 8) bits of the value
		value_size &= 15;
		if (value_size) {
			value = value << value_size | (T)ReadFromCurrent(value_size);
		}
		return value;
	}
private:

	bool LoadedEnough(uint8_t bits) {
		return fill >= bits;
	}
	void Refill() {

		uint8_t additional_load = 0;
		if (bits_read + 16 <= BLOCK_SIZE * 8) {
			current = current << 16 | ReadFromStream() << 8 | ReadFromStream();
			bits_read += 16;
			additional_load = 16;
		}
		else if (bits_read + 8 <= BLOCK_SIZE * 8) {
			current = current << 8 | ReadFromStream() << 8;
			bits_read += 8;
			additional_load = 8;
		}
		fill += additional_load;
	}
	void DecreaseLoadedBits(uint8_t value = 1) {
		fill -= value;
		if (fill < 16 && bits_read + 8 < BLOCK_SIZE * 8) {
			Refill();
		}
	}

	INTERNAL_TYPE ReadFromStream() {
		return stream[stream_index++];
	}

	template <uint8_t VALUE_SIZE>
	uint32_t ReadFromCurrent() {
		assert(fill >= VALUE_SIZE);
		const auto shift_amount = fill - VALUE_SIZE;
		uint32_t result = current >> shift_amount & bitmask<uint64_t>(VALUE_SIZE);
		DecreaseLoadedBits(VALUE_SIZE);
		return result;
	}
	uint32_t ReadFromCurrent(uint8_t value_size) {
		assert(fill >= value_size);
		const auto shift_amount = fill - value_size;
		const auto mask = bitmask<uint32_t>(value_size);
		uint32_t result = (current >> shift_amount) & mask;
		DecreaseLoadedBits(value_size);
		return result;
	}
private:
	INTERNAL_TYPE* stream;	//! The stream we're writing our output to

	uint64_t bits_read = 0;
	uint32_t current;		//! The current value we're reading from (bit buffer)
	uint8_t	fill;			//! How many bits of 'current' are "full"
	size_t stream_index;	//! Index used to keep track of which index we're at in the stream
};

} //namespace duckdb_chimp
