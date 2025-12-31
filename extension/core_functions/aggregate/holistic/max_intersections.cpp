#include "core_functions/aggregate/distributive_functions.hpp"
#include <algorithm>
#include <iterator>
#include <vector>
#include "vergesort.h"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

namespace {

struct MaxIntersectionsState {
	std::vector<int64_t> starts;
	std::vector<int64_t> ends;

	MaxIntersectionsState() {
		starts.reserve(64);
		ends.reserve(64);
	}

	MaxIntersectionsState(MaxIntersectionsState &&other) noexcept
	    : starts(std::move(other.starts)), ends(std::move(other.ends)) {
	}

	MaxIntersectionsState &operator=(MaxIntersectionsState &&other) noexcept {
		if (this != &other) {
			starts = std::move(other.starts);
			ends = std::move(other.ends);
		}
		return *this;
	}

	MaxIntersectionsState(const MaxIntersectionsState &) = delete;
	MaxIntersectionsState &operator=(const MaxIntersectionsState &) = delete;
};

struct MaxIntersectionsFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) MaxIntersectionsState();
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~MaxIntersectionsState();
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &left, const B_TYPE &right, AggregateBinaryInput &input) {
		// Validate interval and add only if valid (left <= right)
		if (left <= right) {
			state.starts.emplace_back(static_cast<int64_t>(left));
			state.ends.emplace_back(static_cast<int64_t>(right) +
			                        1); // Use exclusive end for correct overlap calculation
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const A_TYPE &left, const B_TYPE &right, AggregateBinaryInput &input,
	                              idx_t count) {
		// For constant operations, add the interval once per count if valid
		if (left <= right) {
			state.starts.reserve(state.starts.size() + count);
			state.ends.reserve(state.ends.size() + count);

			int64_t start_val = static_cast<int64_t>(left);
			int64_t end_val = static_cast<int64_t>(right) + 1;

			for (idx_t i = 0; i < count; i++) {
				state.starts.emplace_back(start_val);
				state.ends.emplace_back(end_val);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		if (source.starts.empty()) {
			return;
		}

		target.starts.reserve(target.starts.size() + source.starts.size());
		target.ends.reserve(target.ends.size() + source.ends.size());

		if (aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
			auto &mutable_source = const_cast<STATE &>(source);
			target.starts.insert(target.starts.end(), std::make_move_iterator(mutable_source.starts.begin()),
			                     std::make_move_iterator(mutable_source.starts.end()));
			target.ends.insert(target.ends.end(), std::make_move_iterator(mutable_source.ends.begin()),
			                   std::make_move_iterator(mutable_source.ends.end()));
		} else {
			target.starts.insert(target.starts.end(), source.starts.begin(), source.starts.end());
			target.ends.insert(target.ends.end(), source.ends.begin(), source.ends.end());
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.starts.empty()) {
			target = 0;
			return;
		}

		const size_t num_intervals = state.starts.size();

		if (num_intervals == 1) {
			target = 1;
			return;
		}

		// Define fallback functions for ska_sort
		auto start_fallback = [](std::vector<int64_t>::iterator fb_begin, std::vector<int64_t>::iterator fb_end) {
			std::sort(fb_begin, fb_end);
		};

		auto end_fallback = [](std::vector<int64_t>::iterator fb_begin, std::vector<int64_t>::iterator fb_end) {
			std::sort(fb_begin, fb_end);
		};

		// Use parallel sorting when dataset is large enough to benefit from threading
		const size_t PARALLEL_SORT_THRESHOLD = 10000;

		if (num_intervals >= PARALLEL_SORT_THRESHOLD) {
#ifndef DUCKDB_NO_THREADS
			bool use_parallel = true;
			try {
				// Create threads for parallel sorting
				thread starts_thread([&]() {
					duckdb_vergesort::vergesort(state.starts.begin(), state.starts.end(), std::less<int64_t>(),
					                            start_fallback);
				});

				thread ends_thread([&]() {
					duckdb_vergesort::vergesort(state.ends.begin(), state.ends.end(), std::less<int64_t>(),
					                            end_fallback);
				});

				starts_thread.join();
				ends_thread.join();
			} catch (...) {
				use_parallel = false;
			}

			if (!use_parallel) {
#endif
				duckdb_vergesort::vergesort(state.starts.begin(), state.starts.end(), std::less<int64_t>(),
				                            start_fallback);
				duckdb_vergesort::vergesort(state.ends.begin(), state.ends.end(), std::less<int64_t>(), end_fallback);
#ifndef DUCKDB_NO_THREADS
			}
#endif
		} else {
			duckdb_vergesort::vergesort(state.starts.begin(), state.starts.end(), std::less<int64_t>(), start_fallback);
			duckdb_vergesort::vergesort(state.ends.begin(), state.ends.end(), std::less<int64_t>(), end_fallback);
		}

		size_t start_idx = 0, end_idx = 0;
		int64_t current_count = 0;
		int64_t max_count = 0;

		// Process all events in chronological order
		while (start_idx < num_intervals || end_idx < num_intervals) {
			bool process_start;

			if (start_idx >= num_intervals) {
				process_start = false;
			} else if (end_idx >= num_intervals) {
				process_start = true;
			} else {
				process_start = state.starts[start_idx] < state.ends[end_idx];
			}

			if (process_start) {
				current_count++;
				start_idx++;
				max_count = MaxValue(max_count, current_count);
			} else {
				current_count--;
				end_idx++;
			}
		}

		target = max_count;
	}

	static bool IgnoreNull() {
		return true;
	}
};

} // namespace

AggregateFunction MaxIntersectionsFun::GetFunction() {
	auto function = AggregateFunction::BinaryAggregate<MaxIntersectionsState, int64_t, int64_t, int64_t,
	                                                   MaxIntersectionsFunction, AggregateDestructorType::LEGACY>(
	    LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT);

	function.name = "max_intersections";
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetStateDestructorCallback(
	    AggregateFunction::StateDestroy<MaxIntersectionsState, MaxIntersectionsFunction>);

	return function;
}

} // namespace duckdb
