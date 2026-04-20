#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

AdaptiveFilter::AdaptiveFilter(const Expression &expr) : observe_interval(10), execute_interval(20), warmup(true) {
	auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
	D_ASSERT(conj_expr.children.size() > 1);
	for (idx_t idx = 0; idx < conj_expr.children.size(); idx++) {
		config.permutation.push_back(idx);
		if (conj_expr.children[idx]->CanThrow()) {
			config.disable_permutations = true;
		}
		if (idx != conj_expr.children.size() - 1) {
			config.swap_likeliness.push_back(100);
		}
	}
	right_random_border = 100 * (conj_expr.children.size() - 1);
}

AdaptiveFilter::AdaptiveFilter(const TableFilterSet &table_filters)
    : observe_interval(10), execute_interval(20), warmup(true) {
	config.permutation = ExpressionHeuristics::GetInitialOrder(table_filters);
	for (idx_t idx = 1; idx < table_filters.FilterCount(); idx++) {
		config.swap_likeliness.push_back(100);
	}
	right_random_border = 100 * (table_filters.FilterCount() - 1);
}

AdaptiveFilter::AdaptiveFilter(const TableFilterSet &table_filters, AdaptiveFilterConfiguration seed)
    : config(std::move(seed)), observe_interval(10), execute_interval(20), warmup(false) {
	D_ASSERT(config.permutation.size() == table_filters.FilterCount());
	D_ASSERT(config.swap_likeliness.size() + 1 == table_filters.FilterCount() || table_filters.FilterCount() == 0);
	right_random_border = 100 * (table_filters.FilterCount() - 1);
}

const char *AdaptiveFilterSourceToString(AdaptiveFilterSource source) {
	switch (source) {
	case AdaptiveFilterSource::INITIAL:
		return "initial";
	case AdaptiveFilterSource::SEEDED:
		return "seeded";
	}
	return "unknown";
}

void AdaptiveFilter::SetLogger(Logger &logger_p, string file_path, AdaptiveFilterSource source) {
	logger = &logger_p;
	log_file_path = std::move(file_path);
	LogEvent("INIT", {{"source", AdaptiveFilterSourceToString(source)}});
}

void AdaptiveFilter::LogEvent(const char *event, const vector<pair<string, string>> &info) {
	if (!logger) {
		return;
	}
	if (!logger->ShouldLog(AdaptiveFilterLogType::NAME, AdaptiveFilterLogType::LEVEL)) {
		return;
	}
	auto filter_id = to_string(reinterpret_cast<uintptr_t>(this));
	auto msg = AdaptiveFilterLogType::ConstructLogMessage(filter_id, event, log_file_path, config.permutation, info);
	logger->WriteLog(AdaptiveFilterLogType::NAME, AdaptiveFilterLogType::LEVEL, msg);
}

AdaptiveFilterState AdaptiveFilter::BeginFilter() const {
	if (config.permutation.size() <= 1 || config.disable_permutations) {
		return AdaptiveFilterState();
	}
	AdaptiveFilterState state;
	state.start_time = high_resolution_clock::now();
	return state;
}

void AdaptiveFilter::EndFilter(AdaptiveFilterState state) {
	if (config.permutation.size() <= 1 || config.disable_permutations) {
		// nothing to permute
		return;
	}
	auto end_time = high_resolution_clock::now();
	AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - state.start_time).count());
}

void AdaptiveFilter::AdaptRuntimeStatistics(double duration) {
	iteration_count++;
	runtime_sum += duration;

	D_ASSERT(!config.disable_permutations);
	if (!warmup) {
		// the last swap was observed
		if (observe && iteration_count == observe_interval) {
			// keep swap if runtime decreased, else reverse swap
			auto trial_mean = runtime_sum / static_cast<double>(iteration_count);
			string action;
			if (prev_mean - trial_mean <= 0) {
				// reverse swap because runtime didn't decrease
				std::swap(config.permutation[swap_idx], config.permutation[swap_idx + 1]);

				// decrease swap likeliness, but make sure there is always a small likeliness left
				if (config.swap_likeliness[swap_idx] > 1) {
					config.swap_likeliness[swap_idx] /= 2;
				}
				action = "reverted";
			} else {
				// keep swap because runtime decreased, reset likeliness
				config.swap_likeliness[swap_idx] = 100;
				action = "kept";
			}
			LogEvent("REORDER", {{"action", action},
			                     {"swap_idx", to_string(swap_idx)},
			                     {"prev_mean_us", StringUtil::Format("%.3f", prev_mean * 1e6)},
			                     {"trial_mean_us", StringUtil::Format("%.3f", trial_mean * 1e6)}});
			observe = false;

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		} else if (!observe && iteration_count == execute_interval) {
			// save old mean to evaluate swap
			prev_mean = runtime_sum / static_cast<double>(iteration_count);

			// get swap index and swap likeliness
			// a <= i <= b
			auto random_number = generator.NextRandomInteger(1, NumericCast<uint32_t>(right_random_border));

			swap_idx = random_number / 100;                    // index to be swapped
			idx_t likeliness = random_number - 100 * swap_idx; // random number between [0, 100)

			// check if swap is going to happen
			if (config.swap_likeliness[swap_idx] > likeliness) { // always true for the first swap of an index
				// swap
				std::swap(config.permutation[swap_idx], config.permutation[swap_idx + 1]);

				// observe whether swap will be applied
				observe = true;
			}

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		}
	} else {
		if (iteration_count == 5) {
			// initially set all values
			iteration_count = 0;
			runtime_sum = 0.0;
			observe = false;
			warmup = false;
		}
	}
}

} // namespace duckdb
