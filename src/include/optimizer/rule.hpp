//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parser.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
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

class OptimizerNodeExpressionSet : public OptimizerNode {
  public:
	std::vector<ExpressionType> types;
	OptimizerNodeExpressionSet(std::vector<ExpressionType> types)
	    : types(types) {}
	virtual bool Matches(AbstractExpression &rel) {
		return std::find(types.begin(), types.end(), rel.type) != types.end();
	}
};

class OptimizerNodeExpression : public OptimizerNode {
  public:
	ExpressionType type;
	OptimizerNodeExpression(ExpressionType type) : type(type) {}
	virtual bool Matches(AbstractExpression &rel) { return rel.type == type; }
};

class OptimizerNodeAny : public OptimizerNode {
  public:
	virtual bool Matches(AbstractExpression &rel) { return true; }
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
