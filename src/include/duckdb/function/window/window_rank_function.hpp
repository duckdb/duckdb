//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_rank_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_executor.hpp"

namespace duckdb {

class WindowPeerExecutor : public WindowExecutor {
public:
	static void GetSharing(WindowExecutor &executor, WindowSharedExpressions &shared);

	WindowPeerExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared) : WindowExecutor(wexpr, shared) {
	}

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &context, const idx_t payload_count,
	                                           const ValidityMask &partition_mask,
	                                           const ValidityMask &order_mask) const override;
};

class WindowRankExecutor : public WindowPeerExecutor {
public:
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);

	WindowRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowPeerExecutor(wexpr, shared) {
	}

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

class WindowDenseRankExecutor : public WindowPeerExecutor {
public:
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);

	WindowDenseRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowPeerExecutor(wexpr, shared) {
	}

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

class WindowPercentRankExecutor : public WindowPeerExecutor {
public:
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);

	WindowPercentRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowPeerExecutor(wexpr, shared) {
	}

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

class WindowCumeDistExecutor : public WindowPeerExecutor {
public:
	static void GetBounds(WindowBoundsSet &required, const BoundWindowExpression &wexpr);

	WindowCumeDistExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
	    : WindowPeerExecutor(wexpr, shared) {
	}

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

} // namespace duckdb
