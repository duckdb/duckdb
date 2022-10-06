//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/patas/patas.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/patas/algorithm/patas.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

using byte_index_t = uint32_t;

//! FIXME: replace ChimpType with this
template <class T>
struct FloatingToExact {};

template <>
struct FloatingToExact<double> {
	typedef uint64_t type;
};

template <>
struct FloatingToExact<float> {
	typedef uint32_t type;
};

class ChimpPrimitives {
public:
	static constexpr uint32_t CHIMP_SEQUENCE_SIZE = 1024;
	static constexpr uint8_t MAX_BYTES_PER_VALUE = sizeof(double) + 1; // extra wiggle room
	static constexpr uint8_t CACHELINE_SIZE = 64;
	static constexpr uint8_t HEADER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t FLAG_BIT_SIZE = 2;
	static constexpr uint32_t LEADING_ZERO_BLOCK_BUFFERSIZE = 385;
};

//! Where all the magic happens
template <class T, bool EMPTY>
struct PatasState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	PatasState(void *state_p = nullptr) : data_ptr(state_p), patas_state() {
	}
	//! The Compress/Analyze State
	void *data_ptr;
	PatasCompressionState<EXACT_TYPE, EMPTY> patas_state;

public:
	void AssignDataBuffer(uint8_t *data_out) {
		patas_state.output.SetStream(data_out);
	}

	void AssignFlagBuffer(uint8_t *flag_out) {
		patas_state.flag_buffer.SetBuffer(flag_out);
	}

	void AssignPackedDataBuffer(uint16_t *packed_data_out) {
		patas_state.packed_data_buffer.SetBuffer(packed_data_out);
	}

	void AssignLeadingZeroBuffer(uint8_t *leading_zero_out) {
		patas_state.leading_zero_buffer.SetBuffer(leading_zero_out);
	}

	template <class OP>
	void Flush() {
		patas_state.output.Flush();
	}

	template <class OP>
	bool Update(T uncompressed_value, bool is_valid) {
		OP::template Operation<T>(uncompressed_value, is_valid, data_ptr);
		return true;
	}
};

} // namespace duckdb
