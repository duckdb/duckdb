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

struct OptimizerNode {
	ExpressionType root;
	std::vector<OptimizerNode> children;
	ChildPolicy child_policy;

	OptimizerNode(ExpressionType root)
	    : root(root), child_policy(ChildPolicy::UNORDERED) {}
	OptimizerNode() : child_policy(ChildPolicy::ANY) {}
};

class OptimizerRule {
  public:
	OptimizerNode root;
	virtual std::unique_ptr<AbstractExpression>
	Apply(AbstractExpression &root,
	      std::vector<AbstractExpression *> &bindings) = 0;
	virtual ~OptimizerRule() {}
};

} // namespace duckdb
