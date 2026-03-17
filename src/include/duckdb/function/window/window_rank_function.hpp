//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_rank_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_executor.hpp"
#include "duckdb/function/window_function.hpp"

namespace duckdb {

class WindowPeerExecutor : public WindowExecutor {
public:
	WindowPeerExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &context, const idx_t payload_count,
	                                           const ValidityMask &partition_mask,
	                                           const ValidityMask &order_mask) const override;

	//! The column indices of any ORDER BY argument expressions
	vector<column_t> arg_order_idx;
};

struct RankFunc {
	static WindowFunction GetFunction();
};

class WindowRankExecutor : public WindowPeerExecutor {
public:
	WindowRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

struct DenseRankFun {
	static WindowFunction GetFunction();
};

struct RankDenseFun {
	static WindowFunction GetFunction();
};

class WindowDenseRankExecutor : public WindowPeerExecutor {
public:
	WindowDenseRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

struct PercentRankFun {
	static WindowFunction GetFunction();
};

class WindowPercentRankExecutor : public WindowPeerExecutor {
public:
	WindowPercentRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

struct CumeDistFun {
	static WindowFunction GetFunction();
};

class WindowCumeDistExecutor : public WindowPeerExecutor {
public:
	WindowCumeDistExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

} // namespace duckdb
