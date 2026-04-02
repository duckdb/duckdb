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

	ClientContext &GetContext() {
		D_ASSERT(executor);
		return executor->GetContext();
	}

	unique_ptr<ExpressionExecutor> executor;
};

} // namespace duckdb
