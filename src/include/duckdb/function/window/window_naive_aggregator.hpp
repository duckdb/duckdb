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
	WindowNaiveAggregator(const WindowAggregateExecutor &executor, WindowSharedExpressions &shared);
	~WindowNaiveAggregator() override;

	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;

	//! The parent executor
	const WindowAggregateExecutor &executor;
	//! The column indices of any ORDER BY argument expressions
	vector<column_t> arg_order_idx;
};

} // namespace duckdb
