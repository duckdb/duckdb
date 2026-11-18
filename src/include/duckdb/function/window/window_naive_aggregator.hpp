//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_naive_aggregator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_aggregator.hpp"

namespace duckdb {

class WindowAggregateExecutor;

// Used for validation
class WindowNaiveAggregator : public WindowAggregator {
public:
	WindowNaiveAggregator(const WindowAggregateExecutor &executor, WindowSharedExpressions &shared,
	                      ClientContext &client);
	~WindowNaiveAggregator() override;

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;
	void Evaluate(ExecutionContext &context, const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx,
	              OperatorSinkInput &sink) const override;

	static bool CanAggregate(const BoundWindowExpression &wexpr);

	//! The parent executor
	const WindowAggregateExecutor &executor;
	//! Whether two rows have the same arguments, over [left arguments..., right arguments...] (can be nullptr)
	unique_ptr<Expression> args_equal;
	//! The column indices of any ORDER BY argument expressions
	vector<column_t> arg_order_idx;
};

} // namespace duckdb
