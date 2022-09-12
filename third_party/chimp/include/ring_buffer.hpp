#pragma once

namespace duckdb_chimp {

class RingBuffer {
public:
	static constexpr uint8_t RING_SIZE = 128;
	static constexpr uint64_t LEAST_SIGNIFICANT_BIT_MASK = ((uint64_t)1 << (6 + 7 + 1)) - 1;
	//this.indices = new int[(int) Math.pow(2, threshold + 1)];
	//! Since threshold is now always set to (6 + 7), we can hardcode this to
	static constexpr uint16_t INDICES_SIZE = 1 << (6 + 7 + 1); //16384
public:
	//! Start it at the end, so the first insertion inserts at 0 
	RingBuffer() : buffer_index(RING_SIZE-1) {
	}
	void Insert(double value) {
		buffer_index = (buffer_index + 1) & RING_SIZE;
		buffer[buffer_index] = value;
		indices[IndexOf(value)] = index++;
	}
	const uint64_t& Top() const {
		return (uint64_t)buffer[buffer_index];
	}
	//! Get the index where values that produce this 'key' are stored
	const uint64_t& IndexOf(uint64_t key) const {
		return indices[key];
	}
	//! Get the value at position 'index' of the buffer
	const uint64_t Value(uint8_t index_p) {
		return buffer[index_p];
	}
	//! Get the amount of values that are inserted
	uint64_t Size() const {
		return index - 1;
	}
	uint64_t Key(double value) const {
		return (uint64_t)value & LEAST_SIGNIFICANT_BIT_MASK;
	}

private:
	uint64_t buffer[RING_SIZE] = {}; //! Stores the corresponding values
	uint64_t index = 0; //! Keeps track of the index of the current value
	uint8_t buffer_index = 0; //Circular between 0 and SIZE-1
	uint64_t indices[INDICES_SIZE] = {}; //! Stores the corresponding indices
};

} //namespace duckdb
