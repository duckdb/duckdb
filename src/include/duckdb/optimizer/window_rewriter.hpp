//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/window_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class WindowRewriter : public BaseColumnPruner {
public:
	explicit WindowRewriter(Optimizer &optimizer);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);
	bool CanOptimize(LogicalOperator &op);
	unique_ptr<LogicalOperator> RewriteGet(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);

private:
	Optimizer &optimizer;
	bool lhs_window;
};

} // namespace duckdb
