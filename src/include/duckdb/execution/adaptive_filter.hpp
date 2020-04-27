//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/adaptive_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression/list.hpp"

#include <random>
namespace duckdb {
class TableFilter;

class AdaptiveFilter {
public:
	AdaptiveFilter(Expression &expr);
	AdaptiveFilter(unordered_map<idx_t, vector<TableFilter>> &table_filters);
	void AdaptRuntimeStatistics(double duration);
	vector<idx_t> permutation;

private:
	//! used for adaptive expression reordering
	idx_t iteration_count;
	idx_t swap_idx;
	idx_t right_random_border;
	idx_t observe_interval;
	idx_t execute_interval;
	double runtime_sum;
	double prev_mean;
	bool observe;
	bool warmup;
	vector<idx_t> swap_likeliness;
	std::default_random_engine generator;
};
} // namespace duckdb
