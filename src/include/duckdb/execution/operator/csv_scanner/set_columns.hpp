//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/set_columns.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {
//! This represents the data related to columns that have been set by the user
//! e.g., from a copy command
struct SetColumns {
	SetColumns(const vector<LogicalType> *types_p, const vector<string> *names_p);
	SetColumns();
	//! Return Types that were detected
	const vector<LogicalType> *types = nullptr;
	//! Column Names that were detected
	const vector<string> *names = nullptr;
	//! If columns are set
	bool IsSet() const;
	//! How many columns
	idx_t Size() const;
	//! Helper function that checks if candidate is acceptable based on the number of columns it produces
	bool IsCandidateUnacceptable(const idx_t num_cols, bool null_padding, bool ignore_errors,
	                             bool last_value_always_empty) const;

	string ToString() const;
};

} // namespace duckdb
