//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

//! Thread-local state for executing a table filter
struct TableFilterState {
public:
	virtual ~TableFilterState() = default;

public:
	static unique_ptr<TableFilterState> Initialize(ClientContext &context, const TableFilter &filter);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

enum class ExpressionFilterFastPath : uint8_t {
	NONE,
	IS_OPTIONAL,
	CONSTANT_COMPARISON,
	IS_NULL,
	IS_NOT_NULL,
	BLOOM_FILTER,
	SELECTIVITY_OPTIONAL,
	PERFECT_HASH_JOIN,
	PREFIX_RANGE,
	DYNAMIC_FILTER
};
enum class ExpressionFilterSelectivityStatus : uint8_t { ACTIVE, PAUSED_DUE_TO_HIGH_SELECTIVITY };

struct SelectivityTrackingState {
public:
	bool HasSelectivityTracking() const {
		return n_vectors_to_check != 0;
	}
	bool IsActive() const {
		return selectivity_status == ExpressionFilterSelectivityStatus::ACTIVE;
	}
	void Enable(float selectivity_threshold_p, idx_t n_vectors_to_check_p) {
		selectivity_threshold = selectivity_threshold_p;
		n_vectors_to_check = n_vectors_to_check_p;
	}
	double GetSelectivity() const {
		if (tuples_processed == 0) {
			return 0.0;
		}
		return static_cast<double>(tuples_accepted) / static_cast<double>(tuples_processed);
	}
	void Update(idx_t accepted, idx_t processed) {
		if (!HasSelectivityTracking()) {
			return;
		}
		vectors_processed++;
		tuples_accepted += accepted;
		tuples_processed += processed;

		static constexpr idx_t VECTOR_PAUSE = 10;
		D_ASSERT(n_vectors_to_check < VECTOR_PAUSE);
		if (vectors_processed == MaxValue<idx_t>(pause_multiplier, 1) * VECTOR_PAUSE) {
			vectors_processed = 0;
			tuples_accepted = 0;
			tuples_processed = 0;
			selectivity_status = ExpressionFilterSelectivityStatus::ACTIVE;
		} else if (vectors_processed >= n_vectors_to_check) {
			if (GetSelectivity() >= selectivity_threshold) {
				selectivity_status = ExpressionFilterSelectivityStatus::PAUSED_DUE_TO_HIGH_SELECTIVITY;
				pause_multiplier++;
			} else {
				pause_multiplier = 0;
			}
		}
	}

public:
	float selectivity_threshold = 0;
	idx_t n_vectors_to_check = 0;
	idx_t tuples_accepted = 0;
	idx_t tuples_processed = 0;
	idx_t vectors_processed = 0;
	ExpressionFilterSelectivityStatus selectivity_status = ExpressionFilterSelectivityStatus::ACTIVE;
	idx_t pause_multiplier = 0;
};

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);

	bool HasChildFilters() const {
		return !child_states.empty();
	}
	bool HasFastPath() const {
		return fast_path != ExpressionFilterFastPath::NONE;
	}
	bool HasSelectivityTracking() const {
		return selectivity.HasSelectivityTracking();
	}
	bool IsSelectivityActive() const {
		return selectivity.IsActive();
	}
	void EnableSelectivityTracking(float selectivity_threshold_p, idx_t n_vectors_to_check_p) {
		selectivity.Enable(selectivity_threshold_p, n_vectors_to_check_p);
	}
	void UpdateSelectivity(idx_t accepted, idx_t processed) {
		selectivity.Update(accepted, processed);
	}
	ClientContext &GetContext() {
		if (executor) {
			return executor->GetContext();
		}
		if (selectivity_child_state) {
			return selectivity_child_state->GetContext();
		}
		D_ASSERT(!child_states.empty());
		return child_states[0]->GetContext();
	}

	ExpressionFilterFastPath fast_path = ExpressionFilterFastPath::NONE;
	ExpressionType comparison_type = ExpressionType::INVALID;
	Value constant;
	SelectivityTrackingState selectivity;
	vector<unique_ptr<ExpressionFilterState>> child_states;
	unique_ptr<ExpressionFilterState> selectivity_child_state;
	unique_ptr<AdaptiveFilter> adaptive_filter;
	unique_ptr<ExpressionExecutor> executor;
};

} // namespace duckdb
