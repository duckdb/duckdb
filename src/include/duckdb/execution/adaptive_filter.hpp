//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/adaptive_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

struct AdaptiveFilterState {
	time_point<high_resolution_clock> start_time;
};

struct AdaptiveFilterConfiguration {
	vector<idx_t> permutation;
	vector<idx_t> swap_likeliness;
	bool disable_permutations = false;
};

class AdaptiveFilter {
public:
	explicit AdaptiveFilter(const Expression &expr);
	explicit AdaptiveFilter(const TableFilterSet &table_filters);
	AdaptiveFilter(const TableFilterSet &table_filters, AdaptiveFilterConfiguration seed);

public:
	void AdaptRuntimeStatistics(double duration);

	AdaptiveFilterState BeginFilter() const;
	void EndFilter(AdaptiveFilterState state);

	const AdaptiveFilterConfiguration &GetConfiguration() const {
		return config;
	}

private:
	AdaptiveFilterConfiguration config;
	//! used for adaptive expression reordering
	idx_t iteration_count = 0;
	idx_t swap_idx = 0;
	idx_t right_random_border = 0;
	idx_t observe_interval = 0;
	idx_t execute_interval = 0;
	double runtime_sum = 0;
	double prev_mean = 0;
	bool observe = false;
	bool warmup = false;
	RandomEngine generator;
};
} // namespace duckdb
