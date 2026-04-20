//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/adaptive_filter.hpp"

namespace duckdb {
class Logger;
class TableFilterSet;

class MultiFileAdaptiveFilterCache {
public:
	unique_ptr<AdaptiveFilterConfiguration> GetConfiguration(idx_t expected_filter_count) const;

	void StoreConfiguration(AdaptiveFilterConfiguration adaptive_state);

private:
	mutable mutex lock;
	AdaptiveFilterConfiguration adaptive_state;
	bool populated = false;
};

//! Construct an AdaptiveFilter for a multi-file scan, seeded from `cache` if it holds a compatible
//! configuration. Attaches the supplied logger and emits the INIT event with the correct source tag.
//! Works whether or not `cache` is null.
unique_ptr<AdaptiveFilter> CreateMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache,
                                                         const TableFilterSet &filters, Logger &logger,
                                                         const string &file_path);

//! Push a reader's learned AdaptiveFilter configuration back to the cache. No-op if `cache` is null.
void StoreMultiFileAdaptiveFilter(optional_ptr<MultiFileAdaptiveFilterCache> cache, const AdaptiveFilter &filter);

} // namespace duckdb
