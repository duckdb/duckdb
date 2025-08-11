//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_value_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_executor.hpp"

namespace duckdb {

// Base class for non-aggregate functions that have a payload
class WindowValueExecutor : public WindowExecutor {
public:
	WindowValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	void Finalize(ExecutionContext &context, WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	              CollectionPtr collection) const override;

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                                     const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(ExecutionContext &context,
	                                                   const WindowExecutorGlobalState &gstate) const override;

	//! The column index of the value column
	column_t child_idx = DConstants::INVALID_INDEX;
	//! The column index of the Nth column
	column_t nth_idx = DConstants::INVALID_INDEX;
	//! The column index of the offset column
	column_t offset_idx = DConstants::INVALID_INDEX;
	//! The column index of the default value column
	column_t default_idx = DConstants::INVALID_INDEX;
	//! The column indices of any ORDER BY argument expressions
	vector<column_t> arg_order_idx;
};

class WindowLeadLagExecutor : public WindowValueExecutor {
public:
	WindowLeadLagExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                                     const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(ExecutionContext &context,
	                                                   const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

class WindowFirstValueExecutor : public WindowValueExecutor {
public:
	WindowFirstValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

class WindowLastValueExecutor : public WindowValueExecutor {
public:
	WindowLastValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

class WindowNthValueExecutor : public WindowValueExecutor {
public:
	WindowNthValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

class WindowFillExecutor : public WindowValueExecutor {
public:
	WindowFillExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	//! Never ignore nulls (that's the point!)
	bool IgnoreNulls() const override {
		return false;
	}

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                                     const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(ExecutionContext &context,
	                                                   const WindowExecutorGlobalState &gstate) const override;

	//! Secondary order collection index
	idx_t order_idx = DConstants::INVALID_INDEX;

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

} // namespace duckdb
