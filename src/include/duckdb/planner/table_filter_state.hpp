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

struct ConjunctionAndFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

struct ConjunctionOrFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

enum class ExpressionFilterFastPath : uint8_t { NONE, CONSTANT_COMPARISON, IS_NULL, IS_NOT_NULL };

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);

	bool HasChildFilters() const {
		return !child_states.empty();
	}
	bool HasFastPath() const {
		return fast_path != ExpressionFilterFastPath::NONE;
	}
	ClientContext &GetContext() {
		if (executor) {
			return executor->GetContext();
		}
		D_ASSERT(!child_states.empty());
		return child_states[0]->GetContext();
	}

	ExpressionFilterFastPath fast_path = ExpressionFilterFastPath::NONE;
	ExpressionType comparison_type = ExpressionType::INVALID;
	Value constant;
	vector<unique_ptr<ExpressionFilterState>> child_states;
	unique_ptr<AdaptiveFilter> adaptive_filter;
	unique_ptr<ExpressionExecutor> executor;
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

} // namespace duckdb
