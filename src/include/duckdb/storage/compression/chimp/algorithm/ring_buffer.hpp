//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/ring_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp"

namespace duckdb {

template <class CHIMP_TYPE>
class RingBuffer {
public:
	static constexpr uint8_t RING_SIZE = ChimpConstants::BUFFER_SIZE;
	static constexpr uint64_t LEAST_SIGNIFICANT_BIT_COUNT = SignificantBits<CHIMP_TYPE>::size + 7 + 1;
	static constexpr uint64_t LEAST_SIGNIFICANT_BIT_MASK = (1 << LEAST_SIGNIFICANT_BIT_COUNT) - 1;
	static constexpr uint16_t INDICES_SIZE = 1 << LEAST_SIGNIFICANT_BIT_COUNT; // 16384

public:
	void Reset() {
		index = 0;
	}

	RingBuffer() : index(0) {
	}
	template <bool FIRST = false>
	void Insert(uint64_t value) {
		if (!FIRST) {
			index++;
		}
		buffer[index % RING_SIZE] = value;
		indices[Key(value)] = index;
	}
	template <bool FIRST = false>
	void InsertScan(uint64_t value) {
		if (!FIRST) {
			index++;
		}
		buffer[index % RING_SIZE] = value;
	}
	inline const uint64_t &Top() const {
		return buffer[index % RING_SIZE];
	}
	//! Get the index where values that produce this 'key' are stored
	inline const uint64_t &IndexOf(const uint64_t &key) const {
		return indices[key];
	}
	//! Get the value at position 'index' of the buffer
	inline const uint64_t &Value(const uint8_t &index_p) const {
		return buffer[index_p];
	}
	//! Get the amount of values that are inserted
	inline const uint64_t &Size() const {
		return index;
	}
	inline uint64_t Key(const uint64_t &value) const {
		return value & LEAST_SIGNIFICANT_BIT_MASK;
	}

private:
	uint64_t buffer[RING_SIZE] = {};     //! Stores the corresponding values
	uint64_t index = 0;                  //! Keeps track of the index of the current value
	uint64_t indices[INDICES_SIZE] = {}; //! Stores the corresponding indices
};

} // namespace duckdb
