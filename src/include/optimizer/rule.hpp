//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parser.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {

enum class ChildPolicy { ANY, LEAF, SOME, UNORDERED };

class OptimizerNode {
  public:
	ExpressionType root;
	std::vector<std::unique_ptr<OptimizerNode>> children;
	ChildPolicy child_policy;

	OptimizerNode(ExpressionType root)
	    : root(root), child_policy(ChildPolicy::UNORDERED) {}
	OptimizerNode() : child_policy(ChildPolicy::ANY) {}
	virtual bool Matches(AbstractExpression &rel) = 0;
	virtual ~OptimizerNode() {}
};

class OptimizerRule {
  public:
	std::unique_ptr<OptimizerNode> root;
	virtual std::unique_ptr<AbstractExpression>
	Apply(AbstractExpression &root,
	      std::vector<AbstractExpression *> &bindings) = 0;
	virtual ~OptimizerRule() {}
};

} // namespace duckdb
