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

struct ConjunctionAndFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

struct ConjunctionOrFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);

	ExpressionExecutor executor;
};

struct BFTableFilterState final : public TableFilterState {
	idx_t current_capacity;
	Vector hashes_v;
	Vector found_v;
	Vector keys_sliced_v;
	SelectionVector bf_sel;

	explicit BFTableFilterState(const LogicalType &key_logical_type)
	    : current_capacity(STANDARD_VECTOR_SIZE), hashes_v(LogicalType::HASH), found_v(LogicalType::UBIGINT),
	      keys_sliced_v(key_logical_type), bf_sel(STANDARD_VECTOR_SIZE) {
	}
};

enum class SelectivityOptionalFilterStatus {
	ACTIVE,
	PAUSED_DUE_TO_ZONE_MAP_STATS, // todo: use this to disable the filter for one zone map based on CheckStatistics
	PAUSED_DUE_TO_HIGH_SELECTIVITY
};

struct SelectivityOptionalFilterState final : public TableFilterState {
	struct SelectivityStats {
		idx_t tuples_accepted;
		idx_t tuples_processed;
		idx_t vectors_processed;
		idx_t n_vectors_to_check;
		float selectivity_threshold;
		SelectivityOptionalFilterStatus status;

		SelectivityStats(const idx_t n_vectors_to_check, const float selectivity_threshold)
		    : tuples_accepted(0), tuples_processed(0), vectors_processed(0), n_vectors_to_check(n_vectors_to_check),
		      selectivity_threshold(selectivity_threshold), status(SelectivityOptionalFilterStatus::ACTIVE) {
		}

		void Update(const idx_t accepted, const idx_t processed) {
			if (vectors_processed < n_vectors_to_check) {
				tuples_accepted += accepted;
				tuples_processed += processed;
				vectors_processed += 1;

				// pause the filter if we processed enough vectors and the selectivity is too high
				if (vectors_processed == n_vectors_to_check) {
					if (GetSelectivity() >= selectivity_threshold) {
						status = SelectivityOptionalFilterStatus::PAUSED_DUE_TO_HIGH_SELECTIVITY;
					}
				}
			}
		}

		bool IsActive() const {
			return status == SelectivityOptionalFilterStatus::ACTIVE;
		}

		double GetSelectivity() const {
			if (tuples_processed == 0) {
				return 1.0;
			}
			return static_cast<double>(tuples_accepted) / static_cast<double>(tuples_processed);
		}
	};

	unique_ptr<TableFilterState> child_state;
	SelectivityStats stats;

	explicit SelectivityOptionalFilterState(unique_ptr<TableFilterState> child_state, const idx_t n_vectors_to_check,
	                                        const float selectivity_threshold)
	    : child_state(std::move(child_state)), stats(n_vectors_to_check, selectivity_threshold) {
	}
};

} // namespace duckdb
