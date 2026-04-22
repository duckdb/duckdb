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
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

class Logger;

struct AdaptiveFilterState {
	time_point<high_resolution_clock> start_time;
};

struct AdaptiveFilterConfiguration {
	vector<idx_t> permutation;
	vector<idx_t> swap_likeliness;
	bool disable_permutations = false;
	vector<idx_t> filter_global_pos;
};

enum class AdaptiveFilterSource : uint8_t {
	INITIAL,
	SEEDED,
};

class AdaptiveFilter {
public:
	explicit AdaptiveFilter(const Expression &expr);
	explicit AdaptiveFilter(const TableFilterSet &table_filters, vector<idx_t> filter_global_pos = {});
	AdaptiveFilter(const TableFilterSet &table_filters, AdaptiveFilterConfiguration seed);

public:
	void AdaptRuntimeStatistics(double duration);

	bool Remap(const TableFilterSet &new_filters, vector<idx_t> new_ids);

	AdaptiveFilterState BeginFilter() const;
	void EndFilter(AdaptiveFilterState state);

	const AdaptiveFilterConfiguration &GetConfiguration() const {
		return config;
	}

	void SetLogger(Logger &logger, string file_path = "", AdaptiveFilterSource source = AdaptiveFilterSource::INITIAL,
	               const vector<idx_t> &filter_identities = {});

private:
	void LogEvent(const char *event, const vector<pair<string, string>> &info);

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
	optional_ptr<Logger> logger;
	string log_file_path;
};
} // namespace duckdb
