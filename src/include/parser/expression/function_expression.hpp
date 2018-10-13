//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/function_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a function call
class FunctionExpression : public Expression {
  public:
	FunctionExpression(const char *func_name,
	                   std::vector<std::unique_ptr<Expression>> &children)
	    : Expression(ExpressionType::FUNCTION),
	      func_name(StringUtil::Lower(func_name)) {
		for (auto &child : children) {
			AddChild(std::move(child));
		}
	}

	virtual void ResolveType() override {
		Expression::ResolveType();
		if (func_name == "abs") {
			return_type = children[0]->return_type;
		}
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::FUNCTION;
	}

	std::string func_name;

  private:
	std::vector<TypeId> func_arg_types;
};
} // namespace duckdb
