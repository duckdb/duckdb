//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "planner/expression.hpp"
#include "common/exception.hpp"

namespace duckdb {
//! BoundExpression is an intermediate dummy class used by the binder. It is a ParsedExpression but holds an Expression. It represents a successfully bound expression. It is used in the Binder to prevent re-binding of already bound parts when dealing with subqueries.
class BoundExpression : public ParsedExpression {
public:
	BoundExpression(unique_ptr<Expression> expr) : 
		ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(move(expr)) {}

	unique_ptr<Expression> expr;
public:
	string ToString() const override {
		return "BOUND_EXPRESSION";
	}

	unique_ptr<ParsedExpression> Copy() override {
		throw SerializationException("Cannot copy or serialize bound expression");
	}
};
} // namespace duckdb


