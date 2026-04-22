//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/size.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! capacity_t indicates how many values a structure can hold
struct capacity_t {
	explicit capacity_t(idx_t val) : val(val) {
	}

	operator idx_t() const {
		return val;
	} // NOLINT: allow implicit conversion

private:
	idx_t val;
};

//! count_t indicates how many values are currently in a structure
struct count_t {
	explicit count_t(idx_t val) : val(val) {
	}

	operator idx_t() const {
		return val;
	} // NOLINT: allow implicit conversion

private:
	idx_t val;
};

} // namespace duckdb
