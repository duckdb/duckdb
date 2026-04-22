//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
class Logger;
class TableFilterSet;

//! Per-thread cache for learned AdaptiveFilter order.
class MultiFileAdaptiveFilterCache {
public:
	void InitializeAdaptiveFilter(const TableFilterSet &filters,
	                              const vector<MultiFileGlobalIndex> &filter_global_indices,
	                              shared_ptr<Logger> logger, const string &file_path);

	AdaptiveFilter &GetAdaptiveFilter() const {
		if (!filter) {
			throw InternalException(
			    "Filter from MultiFileAdaptiveFilterCache must be initialized by 'InitializeAdaptiveFilter' first.");
		}
		return *filter;
	}

private:
	unique_ptr<AdaptiveFilter> filter;
};

} // namespace duckdb
