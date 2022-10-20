//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/output_bit_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/common/assert.hpp"

#include "duckdb/storage/compression/chimp/algorithm/bit_utils.hpp"

namespace duckdb {

// This class writes arbitrary amounts of bits to a stream
// The way these bits are written is most-significant bit first
// For example if 6 bits are given as:    0b0011 1111
// The bits are written to the stream as: 0b1111 1100
template <bool EMPTY>
class OutputBitStream {
	using INTERNAL_TYPE = uint8_t;

public:
	friend class BitStreamWriter;
	friend class EmptyWriter;
	OutputBitStream()
	    : stream(nullptr), current(0), free_bits(INTERNAL_TYPE_BITSIZE), stream_index(0), bits_written(0) {
	}

public:
	static constexpr uint8_t INTERNAL_TYPE_BITSIZE = sizeof(INTERNAL_TYPE) * 8;

	idx_t BytesWritten() const {
		return (bits_written >> 3) + ((bits_written & 7) != 0);
	}

	idx_t BitsWritten() const {
		return bits_written;
	}

	void Flush() {
		if (free_bits == INTERNAL_TYPE_BITSIZE) {
			// the bit buffer is empty, nothing to write
			return;
		}
		WriteToStream();
	}

	void SetStream(uint8_t *output_stream) {
		stream = output_stream;
		stream_index = 0;
		bits_written = 0;
		free_bits = INTERNAL_TYPE_BITSIZE;
		current = 0;
	}

	uint64_t *Stream() {
		return (uint64_t *)stream;
	}

	idx_t BitSize() const {
		return (stream_index * INTERNAL_TYPE_BITSIZE) + (INTERNAL_TYPE_BITSIZE - free_bits);
	}

	template <class T>
	void WriteRemainder(T value, uint8_t i) {
		if (sizeof(T) * 8 > 32) {
			if (i == 64) {
				WriteToStream(((uint64_t)value >> 56) & 0xFF);
			}
			if (i > 55) {
				WriteToStream(((uint64_t)value >> 48) & 0xFF);
			}
			if (i > 47) {
				WriteToStream(((uint64_t)value >> 40) & 0xFF);
			}
			if (i > 39) {
				WriteToStream(((uint64_t)value >> 32) & 0xFF);
			}
		}
		if (i > 31) {
			WriteToStream((value >> 24) & 0xFF);
		}
		if (i > 23) {
			WriteToStream((value >> 16) & 0xFF);
		}
		if (i > 15) {
			WriteToStream((value >> 8) & 0xFF);
		}
		if (i > 7) {
			WriteToStream(value);
		}
	}

	template <class T, uint8_t VALUE_SIZE>
	void WriteValue(T value) {
		bits_written += VALUE_SIZE;
		if (EMPTY) {
			return;
		}
		if (FitsInCurrent(VALUE_SIZE)) {
			//! If we can write the entire value in one go
			WriteInCurrent<VALUE_SIZE>((INTERNAL_TYPE)value);
			return;
		}
		auto i = VALUE_SIZE - free_bits;
		const uint8_t queue = i & 7;

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
		WriteRemainder<T>(value, i);
	}

	template <class T>
	void WriteValue(T value, const uint8_t &value_size) {
		bits_written += value_size;
		if (EMPTY) {
			return;
		}
		if (FitsInCurrent(value_size)) {
			//! If we can write the entire value in one go
			WriteInCurrent((INTERNAL_TYPE)value, value_size);
			return;
		}
		auto i = value_size - free_bits;
		const uint8_t queue = i & 7;

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
		WriteRemainder<T>(value, i);
	}

private:
	void WriteBit(bool value) {
		auto &byte = GetCurrentByte();
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

	INTERNAL_TYPE &GetCurrentByte() {
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
		D_ASSERT(free_bits >= value);
		free_bits -= value;
		if (free_bits == 0) {
			WriteToStream();
		}
	}
	void WriteInCurrent(INTERNAL_TYPE value, uint8_t value_size) {
		D_ASSERT(INTERNAL_TYPE_BITSIZE >= value_size);
		const auto shift_amount = free_bits - value_size;
		current |= (value & BitUtils<INTERNAL_TYPE>::Mask(value_size)) << shift_amount;
		DecreaseFreeBits(value_size);
	}

	template <uint8_t VALUE_SIZE = INTERNAL_TYPE_BITSIZE>
	void WriteInCurrent(INTERNAL_TYPE value) {
		D_ASSERT(INTERNAL_TYPE_BITSIZE >= VALUE_SIZE);
		const auto shift_amount = free_bits - VALUE_SIZE;
		current |= (value & BitUtils<INTERNAL_TYPE>::Mask(VALUE_SIZE)) << shift_amount;
		DecreaseFreeBits(VALUE_SIZE);
	}

private:
	uint8_t *stream; //! The stream we're writing our output to

	INTERNAL_TYPE current; //! The current value we're writing into (zero-initialized)
	uint8_t free_bits;     //! How many bits are still unwritten in 'current'
	idx_t stream_index;    //! Index used to keep track of which index we're at in the stream

	idx_t bits_written; //! The total amount of bits written to this stream
};

} // namespace duckdb
