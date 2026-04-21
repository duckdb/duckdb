//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
class Logger;
class TableFilterSet;

struct AdaptiveFilterOrderEntry {
	AdaptiveFilterOrderEntry(MultiFileGlobalIndex global_index, TableFilterType filter_type,
	                         idx_t swap_likeliness = 100)
	    : global_index(global_index), filter_type(filter_type), swap_likeliness(swap_likeliness) {
	}

	MultiFileGlobalIndex global_index;
	TableFilterType filter_type;
	//! exploration weight for swapping, same as on adaptive filtering
	idx_t swap_likeliness;

	bool operator==(const AdaptiveFilterOrderEntry &other) const {
		return global_index == other.global_index && filter_type == other.filter_type;
	}
};

class MultiFileAdaptiveFilterCache {
public:
	vector<AdaptiveFilterOrderEntry> GetOrdering() const;
	void StoreOrdering(vector<AdaptiveFilterOrderEntry> ordering);

private:
	mutable mutex lock;
	vector<AdaptiveFilterOrderEntry> ordering;
};

//! Construct an AdaptiveFilter
unique_ptr<AdaptiveFilter> CreateMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache,
                                                         const TableFilterSet &filters,
                                                         const vector<MultiFileGlobalIndex> &filter_global_indices,
                                                         Logger &logger, const string &file_path);

//! Cache an Adaptive Filter
void StoreMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache, const AdaptiveFilter &filter,
                                  const TableFilterSet &filters,
                                  const vector<MultiFileGlobalIndex> &filter_global_indices);

} // namespace duckdb
