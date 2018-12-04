//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// optimizer/optimizer.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "optimizer/rewriter.hpp"

#include "planner/bindcontext.hpp"

#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {

class Optimizer {
  public:
	Optimizer(BindContext &context);

	std::unique_ptr<LogicalOperator>
	Optimize(std::unique_ptr<LogicalOperator> plan);

	Rewriter rewriter;
};

} // namespace duckdb
