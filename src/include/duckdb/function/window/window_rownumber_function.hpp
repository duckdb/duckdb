//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_rownumber_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_executor.hpp"

namespace duckdb {

class WindowRowNumberExecutor : public WindowExecutor {
public:
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);
	static void GetSharing(WindowExecutor &executor, WindowSharedExpressions &shared);

	WindowRowNumberExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowExecutor(wexpr, shared) {
	}

	static unique_ptr<GlobalSinkState> GetGlobal(ClientContext &client, const WindowExecutor &executor,
	                                             const idx_t payload_count, const ValidityMask &partition_mask,
	                                             const ValidityMask &order_mask);
	static unique_ptr<LocalSinkState> GetLocal(ExecutionContext &context, const GlobalSinkState &gstate);

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result,
	                      idx_t row_idx, OperatorSinkInput &sink) const override;
};

// NTILE is just scaled ROW_NUMBER
class WindowNtileExecutor : public WindowRowNumberExecutor {
public:
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);

	WindowNtileExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowRowNumberExecutor(wexpr, shared) {
	}

	static unique_ptr<LocalSinkState> GetLocal(ExecutionContext &context, const GlobalSinkState &gstate);

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result,
	                      idx_t row_idx, OperatorSinkInput &sink) const override;
};

} // namespace duckdb
