#pragma once

#include <stdint.h>

namespace duckdb_chimp {

//! Set this to uint64_t, not sure what setting a double to 0 does on the bit-level
template <class INTERNAL_TYPE>
class InputBitStream {
public:
	InputBitStream(INTERNAL_TYPE* input_stream, size_t stream_size) :
		stream(input_stream),
		capacity(stream_size),
		current(0),
		bit_index(0),
		stream_index(0)
		{}
public:
	static constexpr uint8_t INTERNAL_TYPE_BITSIZE = sizeof(INTERNAL_TYPE) * 8;

	//! The amount of bytes we've read from the stream (ceiling)
	size_t ByteSize() const {
		return (stream_index * sizeof(INTERNAL_TYPE)) + 1;
	}
	//! The amount of bits we've read from the stream
	size_t BitSize() const {
		return (stream_index * INTERNAL_TYPE_BITSIZE) + bit_index;
	}

	template <class T>
	void ReadBit(bool value, T& value, uint8_t bit) {
		auto& byte = GetCurrentByte();

		const auto bit_offset = INTERNAL_TYPE_BITSIZE - 1 - bit_index;
		//! Shift the bit at 'bit_index' to the front
		//! Then check if it's set
		auto current >> (bit_offset) & 1;
		if (current) {
			value |= (T)1 << ((sizeof(T) * 8) - 1 - bit);
		}
		DecreaseLoadedBits();
	}

	template <class T, uint8_t VALUE_SIZE>
	T ReadValue() {
		if (LoadedEnough(VALUE_SIZE)) {
			return ReadFromCurrent<T>(value, value_size);
		}
		T result = 0;
		for (uint8_t i = 0; i < VALUE_SIZE; i++) {
			if (((value >> i) << (BITS - 1 - i)) & 1) {
				ReadBit(true, result, i);
			}
			else {
				ReadBit(false, result, i);
			}
		}
		return result;
	}

	template <class T>
	T ReadValue(uint8_t value_size = (sizeof(T) * 8)) {
		if (LoadedEnough(value_size)) {
			//! We can read the entire value without loading inbetween
			return ReadFromCurrent<T>(value_size);
		}
		T result = 0;
		for (uint8_t i = 0; i < value_size; i++) {
			if (((value >> i) << (BITS - 1 - i)) & 1) {
				ReadBit(true, result, i);
			}
			else {
				ReadBit(false, result, i);
			}
		}
		return result;
	}
private:
	bool LoadedEnough(uint8_t bits) {
		return bit_index + bits < INTERNAL_TYPE_BITSIZE;
	}
	template <class T>
	T GetMask(uint8_t value_size) const {
		return (T)1 << value_size;
	}
	template <class T, uint8_T VALUE_SIZE>
	T GetMask() const {
		return (T)1 << VALUE_SIZE;
	}
	INTERNAL_TYPE& GetCurrentByte() {
		return current;
	}
	void FetchData() {
		current = stream[stream_index++];
		bit_index = 0;
	}
	void DecreaseLoadedBits(uint8_t value = 1) {
		bit_index += value;
		if (bit_index == INTERNAL_TYPE_BITSIZE) {
			FetchData();
		}
	}
	template <class T, uint8_t VALUE_SIZE>
	T ReadFromCurrent() {
		T result = current >> (bit_index + VALUE_SIZE) & GetMask<VALUE_SIZE>() - 1;
		DecreaseLoadedBits(VALUE_SIZE);
		return result;
	}
	template <class T>
	T ReadFromCurrent(uint8_t value_size) {
		T result = current >> (bit_index + value_size) & GetMask(value_size) - 1;
		DecreaseLoadedBits(value_size);
		return result;
	}
private:
	INTERNAL_TYPE* stream;	//! The stream we're writing our output to
	size_t capacity;		//! The total amount of (bytes / sizeof(INTERNAL_TYPE)) are in the stream

	INTERNAL_TYPE current;	//! The current value we're reading from
	uint8_t	bit_index;		//! How many bits are already read from in the current value
	// |-|-|-|-|-|-|-|-|	//! 8 bit value as example
	// |0|1|2|3|4|5|6|7|
	//          ^			//! If 4 bits are already read from, 4 is the value of 'bit_index'
	size_t stream_index;	//! Index used to keep track of which index we're at in the stream
};

} //namespace duckdb_chimp
