//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/bound_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! BoundExpression is an intermediate dummy class used by the binder. It is a ParsedExpression but holds an Expression.
//! It represents a successfully bound expression. It is used in the Binder to prevent re-binding of already bound parts
//! when dealing with subqueries.
class BoundExpression : public ParsedExpression {
public:
	BoundExpression(unique_ptr<Expression> expr, unique_ptr<ParsedExpression> parsed_expr, SQLType sql_type)
	    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(move(expr)),
	      parsed_expr(move(parsed_expr)), sql_type(sql_type) {
	}

	unique_ptr<Expression> expr;
	unique_ptr<ParsedExpression> parsed_expr;
	SQLType sql_type;

public:
	string ToString() const override {
		return expr->ToString();
	}

	bool Equals(const BaseExpression *other) const override {
		return parsed_expr->Equals(other);
	}
	hash_t Hash() const override {
		return parsed_expr->Hash();
	}

	unique_ptr<ParsedExpression> Copy() const override {
		throw SerializationException("Cannot copy or serialize bound expression");
	}
};

} // namespace duckdb
