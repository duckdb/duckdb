//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/selection_result.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

struct SelectionVector;
struct SelectivityOptionalFilterState;
class Vector;

struct ExpressionFilterExecutor {
	virtual ~ExpressionFilterExecutor() = default;

	virtual idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) = 0;
};

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

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);
	~ExpressionFilterState() override;

	ClientContext &GetContext() {
		D_ASSERT(executor);
		return executor->GetContext();
	}

	unique_ptr<ExpressionExecutor> executor;
	unique_ptr<ExpressionFilterExecutor> fast_executor;
	bool bitmap_capable;    // cleared if runtime vectors leave the bitmap path
	DataChunk filter_chunk; // reused filter input chunk
	SelectionResult scratch;
	unique_ptr<SelectivityOptionalFilterState> skip_gate; // pulled-up selectivity_optional state

	bool ShouldSkip();
	void RecordSelectivity(idx_t accepted, idx_t processed);
};

} // namespace duckdb
