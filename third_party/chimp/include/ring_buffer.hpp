#pragma once

#include <stdio.h>
#include <cstring>

namespace duckdb_chimp {

class RingBuffer {
public:
	static constexpr uint8_t RING_SIZE = 128;
	static constexpr uint64_t LEAST_SIGNIFICANT_BIT_MASK = ((uint64_t)1 << (6 + 7 + 1)) - 1;
	//this.indices = new int[(int) Math.pow(2, threshold + 1)];
	//! Since threshold is now always set to (6 + 7), we can hardcode this to
	static constexpr uint16_t INDICES_SIZE = 1 << (6 + 7 + 1); //16384
public:
	void Reset() {
		index = 0;
		std::memset((void*)indices, 0, INDICES_SIZE);
		std::memset((void*)buffer, 0, RING_SIZE);
	}

	RingBuffer() : index(0) {
	}
	template <bool FIRST = false>
	void Insert(uint64_t value) {
		if (!FIRST) {
			index++;
		}
		auto key = Key(value);
		printf("[%llu] FIRST = %s | KEY: %llu | INSERTED_VALUE: %llu\n", index, FIRST ? "True" : "False", key, value);
		buffer[index % RING_SIZE] = value;
		indices[Key(value)] = index;
	}
	const uint64_t& Top() const {
		return buffer[index % RING_SIZE];
	}
	//! Get the index where values that produce this 'key' are stored
	const uint64_t& IndexOf(uint64_t key) const {
		return indices[key];
	}
	//! Get the value at position 'index' of the buffer
	uint64_t Value(uint8_t index_p) {
		return buffer[index_p];
	}
	//! Get the amount of values that are inserted
	uint64_t Size() const {
		return index;
	}
	uint64_t Key(uint64_t value) const {
		return value & LEAST_SIGNIFICANT_BIT_MASK;
	}

private:
	uint64_t buffer[RING_SIZE] = {}; //! Stores the corresponding values
	uint64_t index = 0; //! Keeps track of the index of the current value
	uint64_t indices[INDICES_SIZE] = {}; //! Stores the corresponding indices
};

} //namespace duckdb
