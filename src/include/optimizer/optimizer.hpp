//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/expression_rewriter.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {
class Binder;

class Optimizer {
public:
	Optimizer(Binder &binder, ClientContext &client_context);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	Binder &binder;
	ExpressionRewriter rewriter;
};

} // namespace duckdb
