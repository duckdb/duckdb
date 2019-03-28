//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/tokens.hpp"
#include "planner/expression.hpp"

#include "common/exception.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class SelectNode;

struct BindResult {
	BindResult(string error) : error(error) {
	}
	BindResult(unique_ptr<Expression> expr) : expression(move(expr)) {
	}

	bool HasError() {
		return !error.empty();
	}

	unique_ptr<Expression> expression;
	string error;
};

//! BoundExpression is an intermediate dummy class used by the binder. It is a ParsedExpression but holds an Expression.
//! It represents a successfully bound expression. It is used in the Binder to prevent re-binding of already bound parts
//! when dealing with subqueries.
class BoundExpression : public ParsedExpression {
public:
	BoundExpression(unique_ptr<Expression> expr)
	    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(move(expr)) {
	}

	unique_ptr<Expression> expr;

public:
	string ToString() const override {
		return "BOUND_EXPRESSION";
	}

	unique_ptr<ParsedExpression> Copy() override {
		throw SerializationException("Cannot copy or serialize bound expression");
	}
};

class ExpressionBinder {
public:
	ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder = false);
	virtual ~ExpressionBinder();

	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> &expr, bool root_expression = true);

	bool BoundColumns() {
		return bound_columns;
	}

	string Bind(unique_ptr<ParsedExpression> *expr, uint32_t depth, bool root_expression = false);

protected:
	virtual BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false);

	BindResult BindExpression(CaseExpression &expr, uint32_t depth);
	BindResult BindExpression(CastExpression &expr, uint32_t depth);
	BindResult BindExpression(ColumnRefExpression &expr, uint32_t depth);
	BindResult BindExpression(ComparisonExpression &expr, uint32_t depth);
	BindResult BindExpression(ConjunctionExpression &expr, uint32_t depth);
	BindResult BindExpression(ConstantExpression &expr, uint32_t depth);
	BindResult BindExpression(FunctionExpression &expr, uint32_t depth);
	BindResult BindExpression(OperatorExpression &expr, uint32_t depth);
	BindResult BindExpression(ParameterExpression &expr, uint32_t depth);
	BindResult BindExpression(StarExpression &expr, uint32_t depth);
	// BindResult BindExpression(SubqueryExpression &expr, uint32_t depth);

	// Bind table names to ColumnRefExpressions
	void BindTableNames(ParsedExpression &expr);

	void BindChild(unique_ptr<ParsedExpression> &expr, uint32_t depth, string &error);

protected:
	unique_ptr<Expression> BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr);

	Binder &binder;
	ClientContext &context;
	ExpressionBinder *stored_binder;
	bool bound_columns = false;
};

//! Cast an expression to the specified SQL type if required
unique_ptr<Expression> AddCastToType(unique_ptr<Expression> expr, SQLType target_type);
//! Retrieves an expression from a BoundExpression node
unique_ptr<Expression> GetExpression(unique_ptr<ParsedExpression> &expr);
} // namespace duckdb
