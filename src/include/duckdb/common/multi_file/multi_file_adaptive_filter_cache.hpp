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

struct GlobalPosition {
	MultiFileGlobalIndex global_index;
	TableFilterType filter_type;

	bool operator==(const GlobalPosition &other) const {
		return global_index == other.global_index && filter_type == other.filter_type;
	}
};

//! Per-thread cache for learned AdaptiveFilter order.
class MultiFileAdaptiveFilterCache {
public:
	void InitializeAdaptiveFilter(const TableFilterSet &filters,
	                              const vector<MultiFileGlobalIndex> &filter_global_indices, Logger &logger,
	                              const string &file_path);

	AdaptiveFilter &GetAdaptiveFilter() const {
		if (!filter) {
			throw InternalException(
			    "Filter from MultiFileAdaptiveFilterCache must be initialized by 'InitializeAdaptiveFilter' first.");
		}
		D_ASSERT(filter);
		return *filter;
	}

private:
	unique_ptr<AdaptiveFilter> filter;
	vector<GlobalPosition> positions;
};

} // namespace duckdb
