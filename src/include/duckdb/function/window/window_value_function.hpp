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

	void Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) const override;

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                           const ValidityMask &partition_mask,
	                                           const ValidityMask &order_mask) const override;
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

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

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                           const ValidityMask &partition_mask,
	                                           const ValidityMask &order_mask) const override;
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

class WindowFirstValueExecutor : public WindowValueExecutor {
public:
	WindowFirstValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

class WindowLastValueExecutor : public WindowValueExecutor {
public:
	WindowLastValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

class WindowNthValueExecutor : public WindowValueExecutor {
public:
	WindowNthValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

class WindowFillExecutor : public WindowValueExecutor {
public:
	WindowFillExecutor(BoundWindowExpression &wexpr, ClientContext &client, WindowSharedExpressions &shared);

	//! Never ignore nulls (that's the point!)
	bool IgnoreNulls() const override {
		return false;
	}

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                           const ValidityMask &partition_mask,
	                                           const ValidityMask &order_mask) const override;
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

	//! Secondary order collection index
	idx_t order_idx = DConstants::INVALID_INDEX;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

} // namespace duckdb
