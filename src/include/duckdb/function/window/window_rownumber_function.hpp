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

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                           const ValidityMask &partition_mask,
	                                           const ValidityMask &order_mask) const override;
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

// NTILE is just scaled ROW_NUMBER
class WindowNtileExecutor : public WindowRowNumberExecutor {
public:
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);

	WindowNtileExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowRowNumberExecutor(wexpr, shared) {
	}

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

} // namespace duckdb
