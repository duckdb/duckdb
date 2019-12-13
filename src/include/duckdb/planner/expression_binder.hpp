//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class SelectNode;

class AggregateFunctionCatalogEntry;
class ScalarFunctionCatalogEntry;
class SimpleFunction;

struct BindResult {
	BindResult(string error) : error(error) {
	}
	BindResult(unique_ptr<Expression> expr, SQLType sql_type) : expression(move(expr)), sql_type(sql_type) {
	}

	bool HasError() {
		return !error.empty();
	}

	unique_ptr<Expression> expression;
	SQLType sql_type;
	string error;
};

class ExpressionBinder {
public:
	ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder = false);
	virtual ~ExpressionBinder();

	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> &expr, SQLType *result_type = nullptr,
	                            bool root_expression = true);

	//! Returns whether or not any columns have been bound by the expression binder
	bool BoundColumns() {
		return bound_columns;
	}

	string Bind(unique_ptr<ParsedExpression> *expr, index_t depth, bool root_expression = false);

	// Bind table names to ColumnRefExpressions
	void BindTableNames(ParsedExpression &expr);

	bool BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr);

	//! The target type that should result from the binder. If the result is not of this type, a cast to this type will
	//! be added. Defaults to INVALID.
	SQLType target_type;

protected:
	virtual BindResult BindExpression(ParsedExpression &expr, index_t depth, bool root_expression = false);

	BindResult BindExpression(CaseExpression &expr, index_t depth);
	BindResult BindExpression(CastExpression &expr, index_t depth);
	BindResult BindExpression(ColumnRefExpression &expr, index_t depth);
	BindResult BindExpression(ComparisonExpression &expr, index_t depth);
	BindResult BindExpression(ConjunctionExpression &expr, index_t depth);
	BindResult BindExpression(ConstantExpression &expr, index_t depth);
	BindResult BindExpression(FunctionExpression &expr, index_t depth);
	BindResult BindExpression(OperatorExpression &expr, index_t depth);
	BindResult BindExpression(ParameterExpression &expr, index_t depth);
	BindResult BindExpression(StarExpression &expr, index_t depth);
	BindResult BindExpression(SubqueryExpression &expr, index_t depth);

	void BindChild(unique_ptr<ParsedExpression> &expr, index_t depth, string &error);

	void CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children,
	                             vector<SQLType> &types);

protected:
	static void ExtractCorrelatedExpressions(Binder &binder, Expression &expr);

	virtual BindResult BindFunction(FunctionExpression &expr, ScalarFunctionCatalogEntry *function, index_t depth);
	virtual BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry *function, index_t depth);

	virtual string UnsupportedAggregateMessage();

	Binder &binder;
	ClientContext &context;
	ExpressionBinder *stored_binder;
	bool bound_columns = false;
};

//! Cast an expression to the specified SQL type if required
unique_ptr<Expression> AddCastToType(unique_ptr<Expression> expr, SQLType source_type, SQLType target_type);

} // namespace duckdb
