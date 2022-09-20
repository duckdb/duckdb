#pragma once

#include <stdint.h>
#include "bit_utils.hpp"
#include <assert.h>

namespace duckdb_chimp {

//! Set this to uint64_t, not sure what setting a double to 0 does on the bit-level
class InputBitStream {
public:
	using INTERNAL_TYPE = uint8_t;

	InputBitStream(uint8_t* input_stream, size_t stream_size) :
		stream(input_stream),
		capacity(stream_size),
		current(0),
		fill(0),
		stream_index(0)
		{
			Refill();
			Refill();
		}
public:
	static constexpr uint8_t INTERNAL_TYPE_BITSIZE = sizeof(INTERNAL_TYPE) * 8;

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
			return ReadFromCurrent(value_size);
		}

		value_size -= fill;
		value = ReadFromCurrent(fill);

		i = value_size >> 3;
		while(i-- != 0) {
			value = value << 8 | ReadFromStream();
		}

		value_size &= 7;

		return (value << value_size) | ReadFromCurrent(value_size);
	}
private:

	bool LoadedEnough(uint8_t bits) {
		return fill >= bits;
	}
	void Refill() {
		current = current << 16 | ReadFromStream() << 8 | ReadFromStream();
		fill += 16;
	}
	void DecreaseLoadedBits(uint8_t value = 1) {
		fill -= value;
		if (fill < 16) {
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
		uint32_t result = current >> shift_amount & bitmask<uint64_t>(value_size);
		DecreaseLoadedBits(value_size);
		return result;
	}
private:
	INTERNAL_TYPE* stream;	//! The stream we're writing our output to
	size_t capacity;		//! The total amount of (bytes / sizeof(INTERNAL_TYPE)) are in the stream

	uint32_t current;		//! The current value we're reading from (bit buffer)
	uint8_t	fill;			//! How many bits of 'current' are "full"
	size_t stream_index;	//! Index used to keep track of which index we're at in the stream
};

} //namespace duckdb_chimp
