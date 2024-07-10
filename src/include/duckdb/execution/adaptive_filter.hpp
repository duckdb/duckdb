//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/adaptive_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include <random>

namespace duckdb {

class AdaptiveFilter {
public:
	explicit AdaptiveFilter(const Expression &expr);
	explicit AdaptiveFilter(const TableFilterSet &table_filters);

	vector<idx_t> permutation;

public:
	void AdaptRuntimeStatistics(double duration);

private:
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
	vector<idx_t> swap_likeliness;
	std::default_random_engine generator;
};
} // namespace duckdb
