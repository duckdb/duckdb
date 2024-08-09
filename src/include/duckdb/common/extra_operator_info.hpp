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
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

class ExtraOperatorInfo {
public:
	ExtraOperatorInfo() : file_filters(""), is_sampling_pushed_down(false) {
	}
	ExtraOperatorInfo(ExtraOperatorInfo &extra_info)
	    : file_filters(extra_info.file_filters), is_sampling_pushed_down(extra_info.is_sampling_pushed_down),
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
	//! Whether sampling is pushed down into the table scan
	bool is_sampling_pushed_down;
	//! Sample rate that have been pushed down into the table scan
	double sample_rate;
};

} // namespace duckdb
