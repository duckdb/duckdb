//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rewriter.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {

class Optimizer {
public:
	Optimizer(BindContext &context);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	BindContext &context;
	Rewriter rewriter;
};

} // namespace duckdb
