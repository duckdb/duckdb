//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/chimp/chimp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "chimp128.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

template <class T>
struct ChimpType {};

template <>
struct ChimpType<double> {
	typedef uint64_t type;
};

template <>
struct ChimpType<float> {
	typedef uint32_t type;
};

class ChimpPrimitives {
public:
	static constexpr uint32_t CHIMP_SEQUENCE_SIZE = 1024;
};

//! Where all the magic happens
template <class T, bool EMPTY>
struct ChimpState {
public:
	//! Compress CHIMP_SEQUENCE_SIZE values at once
	ChimpState(void *state_p = nullptr) : data_ptr(state_p), chimp_state() {
	}
	//! The Compress/Analyze State
	void *data_ptr;
	duckdb_chimp::Chimp128CompressionState<EMPTY> chimp_state;

	T compression_buffer[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];
	idx_t compression_buffer_idx = 0;
	bool compression_buffer_validity[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];

public:
	//! Called when either our internal buffer is full or there are no more values left to process
	template <class OP>
	void Flush() {
		OP::template Operation<T>((T *)compression_buffer, (bool *)compression_buffer_validity, compression_buffer_idx,
		                          data_ptr);
		compression_buffer_idx = 0;
	}

	//! Called for every single value that's decompressed
	template <class OP>
	bool Update(T *uncompressed_data, ValidityMask &validity, idx_t idx) {
		if (validity.RowIsValid(idx)) {
			compression_buffer_validity[compression_buffer_idx] = true;
			compression_buffer[compression_buffer_idx++] = uncompressed_data[idx];
		} else {
			compression_buffer_validity[compression_buffer_idx] = false;
			compression_buffer[compression_buffer_idx++] = 0;
		}

		if (compression_buffer_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
			Flush<OP>();
		}
		return true;
	}
};

} // namespace duckdb
