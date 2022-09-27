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
	static constexpr uint8_t MAX_BITS_PER_VALUE = 74;
	static constexpr uint8_t CACHELINE_SIZE = 64;
	static constexpr uint8_t HEADER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t FLAG_BIT_SIZE = 2;
};

//! Where all the magic happens
template <class T, bool EMPTY>
struct ChimpState {
public:
	ChimpState(void *state_p = nullptr) : data_ptr(state_p), chimp_state() {
	}
	//! The Compress/Analyze State
	void *data_ptr;
	duckdb_chimp::Chimp128CompressionState<EMPTY> chimp_state;

public:
	// TODO: take a flag_output, and a leading_zero_output
	void AssignBuffers(uint8_t *data_out) {
		AssignDataBuffer(data_out);
	}

	template <class OP>
	void Flush() {
		chimp_state.output.Flush();
	}

	template <class OP>
	bool Update(T uncompressed_value, bool is_valid) {
		OP::template Operation<T>(uncompressed_value, is_valid, data_ptr);
		return true;
	}

private:
	void AssignDataBuffer(uint8_t *data_out) {
		chimp_state.output.SetStream(data_out);
	}
	void AssignFlagBuffer(uint8_t *flag_out) {
		chimp_state.flag_output.SetStream(flag_out);
	}
	void AssignLeadingZeroBuffer(uint8_t *leading_zero_out) {
		chimp_state.leading_zero_output.SetStream(leading_zero_out);
	}
};

} // namespace duckdb
