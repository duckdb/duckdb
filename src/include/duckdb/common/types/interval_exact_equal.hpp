//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/interval_exact_equal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/interval.hpp"

namespace duckdb {

//! An interval that only equals an interval with the same months, days and micros, for dictionary encoding
struct interval_exact_equal : public interval_t { // NOLINT
	bool operator==(const interval_exact_equal &right) const {
		return months == right.months && days == right.days && micros == right.micros;
	}

	bool operator!=(const interval_exact_equal &right) const {
		return !(*this == right);
	}
};

} // namespace duckdb
