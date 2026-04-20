//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/adaptive_filter.hpp"

namespace duckdb {


class MultiFileAdaptiveFilterCache {
public:
	unique_ptr<AdaptiveFilterConfiguration> GetConfiguration(idx_t expected_filter_count);

	void StoreConfiguration(AdaptiveFilterConfiguration adaptive_state);

private:
	mutex lock;
	AdaptiveFilterConfiguration adaptive_state;
	bool populated = false;
};

} // namespace duckdb
