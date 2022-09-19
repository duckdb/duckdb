#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

namespace duckdb_chimp {

//! Set this to uint64_t, not sure what setting a double to 0 does on the bit-level
template <bool EMPTY>
class OutputBitStream {
	using INTERNAL_TYPE = uint64_t;
public:
	friend class BitStreamWriter;
	friend class EmptyWriter;
	OutputBitStream(uint64_t* output_stream, size_t stream_size) :
		stream(output_stream),
		capacity(stream_size),
		current(0),
		free_bits(INTERNAL_TYPE_BITSIZE),
		stream_index(0),
		bits_written(0)
		{}
public:
	static constexpr uint8_t INTERNAL_TYPE_BITSIZE = sizeof(uint64_t) * 8;

	size_t BitsWritten() const {
		return bits_written;
	}

	//! The amount of bytes we've filled
	size_t ByteSize() const {
		return (stream_index * sizeof(uint64_t)) + 1;
	}
	size_t BitSize() const {
		return (stream_index * INTERNAL_TYPE_BITSIZE) + (INTERNAL_TYPE_BITSIZE - free_bits);
	}

	//! Hopefully the compiler can unroll this since VALUE_SIZE is known at compile time?
	template <class T, uint8_t VALUE_SIZE>
	void WriteValue(T value) {
		printf("value %llu, bits: %d\n", value, VALUE_SIZE);
		if (EMPTY) {
			return;
		}
		if (FitsInCurrent(VALUE_SIZE)) {
			WriteInCurrent<T, VALUE_SIZE>(value);
			return;
		}
		for (uint8_t i = 0; i < VALUE_SIZE; i++) {
			if (((value >> i) << (INTERNAL_TYPE_BITSIZE - 1 - i)) & 1) {
				WriteBit(true);
			}
			else {
				WriteBit(false);
			}
		}
	}
	//TODO: optimize this to be unrolled?
	template <class T>
	void WriteValue(T value, uint8_t value_size) {
		printf("value %llu, bits: %d\n", value, value_size);
		if (EMPTY) {
			return;
		}
		if (FitsInCurrent(value_size)) {
			WriteInCurrent<T>(value, value_size);
			return;
		}
		for (uint8_t i = 0; i < value_size; i++) {
			if (((value >> i) << (INTERNAL_TYPE_BITSIZE - 1 - i)) & 1) {
				WriteBit(true);
			}
			else {
				WriteBit(false);
			}
		}
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
	uint64_t GetMask() const {
		return (uint64_t)1 << free_bits;
	}
	uint64_t& GetCurrentByte() {
		return current;
	}
	void WriteToStream() {
		stream[stream_index++] = current;
		current = 0;
		free_bits = INTERNAL_TYPE_BITSIZE;
	}
	void DecreaseFreeBits(uint8_t value = 1) {
		free_bits -= value;
		if (free_bits == 0) {
			WriteToStream();
		}
	}
	template <class T, uint8_t VALUE_SIZE>
	void WriteInCurrent(T value) {
		if (sizeof(T) * 8 == VALUE_SIZE) {
			current |= (INTERNAL_TYPE)value << (free_bits - VALUE_SIZE);
		}
		else {
			current |= ((INTERNAL_TYPE)value & (((INTERNAL_TYPE)1 << VALUE_SIZE) - 1)) << (free_bits - VALUE_SIZE);
		}
		DecreaseFreeBits(VALUE_SIZE);
	}

	template <class T>
	void WriteInCurrent(T value, uint8_t value_size) {
		if (sizeof(T) * 8 == value_size) {
			current |= (INTERNAL_TYPE)value << (free_bits - value_size);
		}
		else {
			current |= ((INTERNAL_TYPE)value & (((INTERNAL_TYPE)1 << value_size) - 1)) << (free_bits - value_size);
		}
		DecreaseFreeBits(value_size);
	}
private:
	uint64_t* stream;		//! The stream we're writing our output to
	size_t capacity;		//! The total amount of (bytes / sizeof(uint64_t)) are in the stream

	INTERNAL_TYPE current;	//! The current value we're writing into (zero-initialized)
	uint8_t	free_bits;		//! How many bits are still unwritten in 'current'
	size_t stream_index;	//! Index used to keep track of which index we're at in the stream

	size_t bits_written;	//! The total amount of bits written to this stream
};

} //namespace duckdb_chimp
