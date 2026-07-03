//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
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

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);

	ClientContext &GetContext() {
		D_ASSERT(executor);
		return executor->GetContext();
	}

	unique_ptr<ExpressionExecutor> executor;
	//! Statically true for bitmap-eligible expressions; cleared on the first non-bitmap result at runtime
	bool bitmap_capable;
	//! Reused per-call input chunk and result selection, so per-vector filter calls allocate nothing
	DataChunk filter_chunk;
	SelectionVector scratch;
};

} // namespace duckdb
