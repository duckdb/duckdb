
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class FunctionExpression : public AbstractExpression {
  public:
	FunctionExpression(
	    const char *func_name,
	    std::vector<std::unique_ptr<AbstractExpression>> &children)
	    : AbstractExpression(ExpressionType::FUNCTION), func_name(func_name) {
		for (auto &child : children) {
			AddChild(std::move(child));
		}
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

  private:
	std::string func_name;
	std::vector<TypeId> func_arg_types;
};
} // namespace duckdb
