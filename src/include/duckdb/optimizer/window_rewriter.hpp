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
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

class Optimizer;

class WindowRewriter {
public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);
	bool CanOptimize(LogicalOperator &op);
	unique_ptr<LogicalOperator> RewriteGet(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);

	ColumnBindingReplacer replacer;
};

} // namespace duckdb
