//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/algorithm/chimp128.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

using byte_index_t = uint32_t;

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
	static constexpr uint8_t MAX_BYTES_PER_VALUE = sizeof(double) + 1; // extra wiggle room
	static constexpr uint8_t HEADER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t FLAG_BIT_SIZE = 2;
	static constexpr uint32_t LEADING_ZERO_BLOCK_BUFFERSIZE = 1 + (CHIMP_SEQUENCE_SIZE / 8) * 3;
};

//! Where all the magic happens
template <class T, bool EMPTY>
struct ChimpState {
public:
	using CHIMP_TYPE = typename ChimpType<T>::type;

	ChimpState() : chimp() {
	}
	Chimp128CompressionState<CHIMP_TYPE, EMPTY> chimp;

public:
	void AssignDataBuffer(uint8_t *data_out) {
		chimp.output.SetStream(data_out);
	}

	void AssignFlagBuffer(uint8_t *flag_out) {
		chimp.flag_buffer.SetBuffer(flag_out);
	}

	void AssignPackedDataBuffer(uint16_t *packed_data_out) {
		chimp.packed_data_buffer.SetBuffer(packed_data_out);
	}

	void AssignLeadingZeroBuffer(uint8_t *leading_zero_out) {
		chimp.leading_zero_buffer.SetBuffer(leading_zero_out);
	}

	void Flush() {
		chimp.output.Flush();
	}
};

} // namespace duckdb
