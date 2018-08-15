//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/expression_rule.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {

class ExpressionNode {
  public:
	ExpressionType root;
	std::vector<std::unique_ptr<ExpressionNode>> children;
	ChildPolicy child_policy;

	ExpressionNode(ExpressionType root)
	    : root(root), child_policy(ChildPolicy::UNORDERED) {}
	ExpressionNode() : child_policy(ChildPolicy::ANY) {}
	virtual bool Matches(AbstractExpression &rel) = 0;
	virtual ~ExpressionNode() {}
};

class ExpressionNodeSet : public ExpressionNode {
  public:
	std::vector<ExpressionType> types;
	ExpressionNodeSet(std::vector<ExpressionType> types)
	    : types(types) {}
	virtual bool Matches(AbstractExpression &rel) {
		return std::find(types.begin(), types.end(), rel.type) != types.end();
	}
};

class ExpressionNodeType : public ExpressionNode {
  public:
	ExpressionType type;
	ExpressionNodeType(ExpressionType type) : type(type) {}
	virtual bool Matches(AbstractExpression &rel) { return rel.type == type; }
};

class ExpressionNodeAny : public ExpressionNode {
  public:
	virtual bool Matches(AbstractExpression &rel) { return true; }
};

class ExpressionRule {
  public:
	std::unique_ptr<ExpressionNode> root;
	virtual std::unique_ptr<AbstractExpression>
	Apply(AbstractExpression &root,
	      std::vector<AbstractExpression *> &bindings) = 0;
	virtual ~ExpressionRule() {}
};

} // namespace duckdb
