//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/sniffer/sniff_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Struct to store the result of the Sniffer
struct SnifferResult {
	SnifferResult(vector<LogicalType> return_types_p, vector<string> names_p)
	    : return_types(std::move(return_types_p)), names(std::move(names_p)) {
	}
	//! Return Types that were detected
	vector<LogicalType> return_types;
	//! Column Names that were detected
	vector<string> names;
};

struct AdaptiveSnifferResult : SnifferResult {
	AdaptiveSnifferResult(vector<LogicalType> return_types_p, vector<string> names_p, bool more_than_one_row_p)
	    : SnifferResult(std::move(return_types_p), std::move(names_p)), more_than_one_row(more_than_one_row_p) {
	}
	bool more_than_one_row;
	SnifferResult ToSnifferResult() {
		return {return_types, names};
	}
};
} // namespace duckdb
