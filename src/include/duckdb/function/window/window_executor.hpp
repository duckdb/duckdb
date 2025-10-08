//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/function/window/window_boundaries_state.hpp"
#include "duckdb/function/window/window_collection.hpp"

namespace duckdb {

class WindowCollection;

struct WindowSharedExpressions;

class WindowExecutor;

class WindowExecutorGlobalState : public GlobalSinkState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowExecutorGlobalState(ClientContext &client, const WindowExecutor &executor, const idx_t payload_count,
	                          const ValidityMask &partition_mask, const ValidityMask &order_mask);

	ClientContext &client;
	const WindowExecutor &executor;

	const idx_t payload_count;
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	vector<LogicalType> arg_types;
};

class WindowExecutorLocalState : public LocalSinkState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowExecutorLocalState(ExecutionContext &context, const WindowExecutorGlobalState &gstate);

	virtual void Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx,
	                  OperatorSinkInput &sink);
	virtual void Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink);

	//! The state used for reading the range collection
	unique_ptr<WindowCursor> range_cursor;
};

class WindowExecutorBoundsLocalState : public WindowExecutorLocalState {
public:
	WindowExecutorBoundsLocalState(ExecutionContext &context, const WindowExecutorGlobalState &gstate);
	~WindowExecutorBoundsLocalState() override {
	}

	virtual void UpdateBounds(WindowExecutorGlobalState &gstate, idx_t row_idx, DataChunk &eval_chunk,
	                          optional_ptr<WindowCursor> range);

	// Frame management
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	DataChunk bounds;
	WindowBoundariesState state;
};

class WindowExecutor {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);
	virtual ~WindowExecutor() {
	}

	virtual bool IgnoreNulls() const;

	virtual unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                                   const ValidityMask &partition_mask,
	                                                   const ValidityMask &order_mask) const;
	virtual unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const;

	virtual void Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
	                  OperatorSinkInput &sink) const;

	virtual void Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) const;

	void Evaluate(ExecutionContext &context, idx_t row_idx, DataChunk &eval_chunk, Vector &result,
	              OperatorSinkInput &sink) const;

	// The function
	const BoundWindowExpression &wexpr;

	// evaluate frame expressions, if needed
	column_t boundary_start_idx = DConstants::INVALID_INDEX;
	column_t boundary_end_idx = DConstants::INVALID_INDEX;

	// evaluate RANGE expressions, if needed
	optional_ptr<Expression> range_expr;
	column_t range_idx = DConstants::INVALID_INDEX;

protected:
	virtual void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count,
	                              idx_t row_idx, OperatorSinkInput &sink) const = 0;
};

} // namespace duckdb
