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

class ExpressionBinder {
public:
	ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder = false);
	virtual ~ExpressionBinder();

	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> &expr);

protected:
	string Bind(unique_ptr<ParsedExpression> *expr, uint32_t depth, bool root_expression = false);

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
	BindResult BindExpression(SubqueryExpression &expr, uint32_t depth);

	// Bind table names to ColumnRefExpressions
	void BindTableNames(ParsedExpression &expr);

	bool BoundColumns() {
		return bound_columns;
	}

protected:
	unique_ptr<Expression> BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr);

	Binder &binder;
	ClientContext &context;
	ExpressionBinder *stored_binder;
	bool bound_columns = false;

private:
	//! Retrieves an expression from a BoundExpression node
	unique_ptr<Expression> GetExpression(ParsedExpression &expr);
	unique_ptr<Expression> AddCastToType(unique_ptr<Expression> expr, SQLType target_type);
};

} // namespace duckdb
