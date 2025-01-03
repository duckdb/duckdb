//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_boundaries_state.hpp"
#include "duckdb/function/window/window_collection.hpp"

namespace duckdb {

class WindowCollection;

struct WindowSharedExpressions;

class WindowExecutorState {
public:
	WindowExecutorState() {};
	virtual ~WindowExecutorState() {
	}

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

class WindowExecutor;

class WindowExecutorGlobalState : public WindowExecutorState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowExecutorGlobalState(const WindowExecutor &executor, const idx_t payload_count,
	                          const ValidityMask &partition_mask, const ValidityMask &order_mask);

	const WindowExecutor &executor;

	const idx_t payload_count;
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	vector<LogicalType> arg_types;
};

class WindowExecutorLocalState : public WindowExecutorState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	explicit WindowExecutorLocalState(const WindowExecutorGlobalState &gstate);

	virtual void Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx);
	virtual void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection);

	//! The state used for reading the range collection
	unique_ptr<WindowCursor> range_cursor;
};

class WindowExecutorBoundsState : public WindowExecutorLocalState {
public:
	explicit WindowExecutorBoundsState(const WindowExecutorGlobalState &gstate);
	~WindowExecutorBoundsState() override {
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

	WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);
	virtual ~WindowExecutor() {
	}

	virtual unique_ptr<WindowExecutorGlobalState>
	GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask, const ValidityMask &order_mask) const;
	virtual unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const;

	virtual void Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
	                  WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const;

	virtual void Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	                      CollectionPtr collection) const;

	void Evaluate(idx_t row_idx, DataChunk &eval_chunk, Vector &result, WindowExecutorLocalState &lstate,
	              WindowExecutorGlobalState &gstate) const;

	// The function
	const BoundWindowExpression &wexpr;
	ClientContext &context;

	// evaluate frame expressions, if needed
	column_t boundary_start_idx = DConstants::INVALID_INDEX;
	column_t boundary_end_idx = DConstants::INVALID_INDEX;

	// evaluate RANGE expressions, if needed
	optional_ptr<Expression> range_expr;
	column_t range_idx = DConstants::INVALID_INDEX;

protected:
	virtual void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const = 0;
};

} // namespace duckdb
