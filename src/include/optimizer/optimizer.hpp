//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/expression_rewriter.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {

class Optimizer {
public:
	Optimizer(ClientContext &client_context);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	ExpressionRewriter rewriter;
};

} // namespace duckdb
