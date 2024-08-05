//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/extra_operator_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

class ExtraOperatorInfo {
public:
	ExtraOperatorInfo() : file_filters(""), sample_rate(1) {
	}
	ExtraOperatorInfo(ExtraOperatorInfo &extra_info) : file_filters(extra_info.file_filters),
	sample_rate(extra_info.sample_rate) {
		if (extra_info.total_files.IsValid()) {
			total_files = extra_info.total_files.GetIndex();
		}
		if (extra_info.filtered_files.IsValid()) {
			filtered_files = extra_info.filtered_files.GetIndex();
		}
	}

	//! Filters that have been pushed down into the main file list
	string file_filters;
	//! Total size of file list
	optional_idx total_files;
	//! Size of file list after applying filters
	optional_idx filtered_files;
	//! Sample rate that have been pushed down into the table scan
	double sample_rate;
};

} // namespace duckdb
