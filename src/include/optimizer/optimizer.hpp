//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/optimizer.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "planner/logical_operator.hpp"
#include "planner/logical_visitor.hpp"
#include "optimizer/rewriter.hpp"

namespace duckdb {

class Optimizer : public LogicalOperatorVisitor {
  public:
	Optimizer();

	std::unique_ptr<LogicalOperator> Optimize(std::unique_ptr<LogicalOperator> plan);

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return message; }

	virtual void Visit(LogicalAggregate &aggregate);
	virtual void Visit(LogicalFilter &filter);
	virtual void Visit(LogicalOrder &order);
	virtual void Visit(LogicalProjection &filter);
  private:
  	void RewriteList(std::vector<std::unique_ptr<AbstractExpression>>& list);

  	ExpressionRewriter rewriter;

	bool success;
	std::string message;
};

} // namespace duckdb
