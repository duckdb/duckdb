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
	static unique_ptr<FunctionData> Bind(BindWindowFunctionInput &input);
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);
	static void GetSharing(WindowExecutor &executor, WindowSharedExpressions &shared);

	WindowValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared) : WindowExecutor(wexpr, shared) {
	}

	void Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) const override;

	static unique_ptr<GlobalSinkState> GetGlobal(ClientContext &client, const WindowExecutor &executor,
	                                             const idx_t payload_count, const ValidityMask &partition_mask,
	                                             const ValidityMask &order_mask);
	static unique_ptr<LocalSinkState> GetLocal(ExecutionContext &context, const GlobalSinkState &gstate);
};

class WindowLeadLagExecutor : public WindowValueExecutor {
public:
	static unique_ptr<FunctionData> Bind(BindWindowFunctionInput &input);

	WindowLeadLagExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowValueExecutor(wexpr, shared) {
	}

	static unique_ptr<GlobalSinkState> GetGlobal(ClientContext &client, const WindowExecutor &executor,
	                                             const idx_t payload_count, const ValidityMask &partition_mask,
	                                             const ValidityMask &order_mask);
	static unique_ptr<LocalSinkState> GetLocal(ExecutionContext &context, const GlobalSinkState &gstate);

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result,
	                      idx_t row_idx, OperatorSinkInput &sink) const override;
};

class WindowFirstValueExecutor : public WindowValueExecutor {
public:
	WindowFirstValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowValueExecutor(wexpr, shared) {
	}

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result,
	                      idx_t row_idx, OperatorSinkInput &sink) const override;
};

class WindowLastValueExecutor : public WindowValueExecutor {
public:
	WindowLastValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowValueExecutor(wexpr, shared) {
	}

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result,
	                      idx_t row_idx, OperatorSinkInput &sink) const override;
};

class WindowNthValueExecutor : public WindowValueExecutor {
public:
	WindowNthValueExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowValueExecutor(wexpr, shared) {
	}

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result,
	                      idx_t row_idx, OperatorSinkInput &sink) const override;
};

class WindowFillExecutor : public WindowValueExecutor {
public:
	static unique_ptr<FunctionData> Bind(BindWindowFunctionInput &input);
	static void Validate(ClientContext &context, WindowFunction &function, vector<unique_ptr<Expression>> &arguments,
	                     vector<OrderByNode> &orders, vector<OrderByNode> &arg_orders);
	static void GetSharing(WindowExecutor &executor, WindowSharedExpressions &shared);

	WindowFillExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowValueExecutor(wexpr, shared) {
	}

	static unique_ptr<GlobalSinkState> GetGlobal(ClientContext &client, const WindowExecutor &executor,
	                                             const idx_t payload_count, const ValidityMask &partition_mask,
	                                             const ValidityMask &order_mask);
	static unique_ptr<LocalSinkState> GetLocal(ExecutionContext &context, const GlobalSinkState &gstate);

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result,
	                      idx_t row_idx, OperatorSinkInput &sink) const override;
};

} // namespace duckdb
