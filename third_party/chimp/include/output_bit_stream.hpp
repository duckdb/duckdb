#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <assert.h>

#include "bit_utils.hpp"

namespace duckdb_chimp {

//! Set this to uint64_t, not sure what setting a double to 0 does on the bit-level
template <bool EMPTY>
class OutputBitStream {
	using INTERNAL_TYPE = uint8_t;
public:
	friend class BitStreamWriter;
	friend class EmptyWriter;
	OutputBitStream(uint8_t* output_stream) :
		stream((INTERNAL_TYPE*)output_stream),
		current(0),
		free_bits(INTERNAL_TYPE_BITSIZE),
		stream_index(0),
		bits_written(0)
		{}
public:
	static constexpr uint8_t INTERNAL_TYPE_BITSIZE = sizeof(INTERNAL_TYPE) * 8;

	size_t BitsWritten() const {
		return bits_written;
	}
	
	uint64_t* Stream() {
		return (uint64_t*)stream;
	}

	//! The amount of bytes we've filled
	size_t ByteSize() const {
		return (stream_index * sizeof(INTERNAL_TYPE)) + 1;
	}
	size_t BitSize() const {
		return (stream_index * INTERNAL_TYPE_BITSIZE) + (INTERNAL_TYPE_BITSIZE - free_bits);
	}

	template <class T, uint8_t VALUE_SIZE>
	void WriteValue(T value) {
		bits_written += VALUE_SIZE;
		printf("value %llu, bits: %d\n", value, VALUE_SIZE);
		if (EMPTY) {
			return;
		}
		if (FitsInCurrent(VALUE_SIZE)) {
			//! If we can write the entire value in one go
			WriteInCurrent<VALUE_SIZE>((INTERNAL_TYPE)value);
			return;
		}
		auto i = VALUE_SIZE - free_bits;
		const uint8_t queue = i & 0b00000111;

		if (free_bits != 0) {
			// Reset the number of free bits
			WriteInCurrent(value >> i, free_bits);
		}
		if (queue != 0) {
			// We dont fill the entire 'current' buffer,
			// so we can write these to 'current' first without flushing to the stream
			// And then write the remaining bytes directly to the stream
			i -= queue;
			WriteInCurrent((INTERNAL_TYPE)value, queue);
			value >>= queue;
		}
		if (i == 64) WriteToStream((INTERNAL_TYPE)(value >> 56));
		if (i > 55) WriteToStream((INTERNAL_TYPE)(value >> 48));
		if (i > 47) WriteToStream((INTERNAL_TYPE)(value >> 40));
		if (i > 39) WriteToStream((INTERNAL_TYPE)(value >> 32));
		if (i > 31) WriteToStream((INTERNAL_TYPE)(value >> 24));
		if (i > 23) WriteToStream((INTERNAL_TYPE)(value >> 16));
		if (i > 15) WriteToStream((INTERNAL_TYPE)(value >> 8));
		if (i > 7) WriteToStream(value);
	}
	template <class T>
	void WriteValue(T value, uint8_t value_size) {
		bits_written += value_size;
		printf("value %llu, bits: %d\n", value, value_size);
		if (EMPTY) {
			return;
		}
		if (FitsInCurrent(value_size)) {
			//! If we can write the entire value in one go
			WriteInCurrent((INTERNAL_TYPE)value, value_size);
			return;
		}
		auto i = value_size - free_bits;
		const uint8_t queue = i & 0b00000111;

		if (free_bits != 0) {
			// Reset the number of free bits
			WriteInCurrent(value >> i, free_bits);
		}
		if (queue != 0) {
			// We dont fill the entire 'current' buffer,
			// so we can write these to 'current' first without flushing to the stream
			// And then write the remaining bytes directly to the stream
			i -= queue;
			WriteInCurrent((INTERNAL_TYPE)value, queue);
			value >>= queue;
		}
		if (i == 64) WriteToStream((INTERNAL_TYPE)(value >> 56));
		if (i > 55) WriteToStream((INTERNAL_TYPE)(value >> 48));
		if (i > 47) WriteToStream((INTERNAL_TYPE)(value >> 40));
		if (i > 39) WriteToStream((INTERNAL_TYPE)(value >> 32));
		if (i > 31) WriteToStream((INTERNAL_TYPE)(value >> 24));
		if (i > 23) WriteToStream((INTERNAL_TYPE)(value >> 16));
		if (i > 15) WriteToStream((INTERNAL_TYPE)(value >> 8));
		if (i > 7) WriteToStream(value);
	}
private:
	void WriteBit(bool value) {
		auto& byte = GetCurrentByte();
		if (value) {
			byte = byte | GetMask();
		}
		DecreaseFreeBits();
	}

	bool FitsInCurrent(uint8_t bits) {
		return free_bits >= bits;
	}
	INTERNAL_TYPE GetMask() const {
		return (INTERNAL_TYPE)1 << free_bits;
	}

	INTERNAL_TYPE& GetCurrentByte() {
		return current;
	}
	//! Write a value of type INTERNAL_TYPE directly to the stream
	void WriteToStream(INTERNAL_TYPE value) {
		stream[stream_index++] = value;
	}
	void WriteToStream() {
		stream[stream_index++] = current;
		current = 0;
		free_bits = INTERNAL_TYPE_BITSIZE;
	}
	void DecreaseFreeBits(uint8_t value = 1) {
		assert(free_bits >= value);
		free_bits -= value;
		if (free_bits == 0) {
			WriteToStream();
		}
	}
	void WriteInCurrent(INTERNAL_TYPE value, uint8_t value_size) {
		assert(INTERNAL_TYPE_BITSIZE >= value_size);
		const auto shift_amount = free_bits - value_size;
		current |= (value & bitmask<INTERNAL_TYPE>(value_size)) << shift_amount;
		DecreaseFreeBits(value_size);
	}

	template <uint8_t VALUE_SIZE = INTERNAL_TYPE_BITSIZE>
	void WriteInCurrent(INTERNAL_TYPE value) {
		assert(INTERNAL_TYPE_BITSIZE >= VALUE_SIZE);
		const auto shift_amount = free_bits - VALUE_SIZE;
		current |= (value & bitmask<INTERNAL_TYPE>(VALUE_SIZE)) << shift_amount;
		DecreaseFreeBits(VALUE_SIZE);
	}

private:
	uint8_t* stream;		//! The stream we're writing our output to

	INTERNAL_TYPE current;	//! The current value we're writing into (zero-initialized)
	uint8_t	free_bits;		//! How many bits are still unwritten in 'current'
	size_t stream_index;	//! Index used to keep track of which index we're at in the stream

	size_t bits_written;	//! The total amount of bits written to this stream
};

} //namespace duckdb_chimp
