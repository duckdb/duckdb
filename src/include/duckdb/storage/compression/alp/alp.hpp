//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alp/algorithm/alp.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

template <class T, bool EMPTY>
struct AlpState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	AlpState(void *state_p = nullptr) : data_ptr(state_p), alp_state() {
	}
	//! The Compress/Analyze State
	void *data_ptr;
	alp::AlpCompressionState<T, EMPTY> alp_state;

};

} // namespace duckdb