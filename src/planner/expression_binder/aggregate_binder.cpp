#include "planner/expression_binder/aggregate_binder.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;


AggregateBinder::AggregateBinder(Binder &binder, ClientContext &context, SelectNode& node) : 
	SelectNodeBinder(binder, context, node, true), bound_columns(false) {
}

AggregateBinder::~AggregateBinder() {
}

BindResult AggregateBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	switch(expr->GetExpressionClass()) {
		case ExpressionClass::AGGREGATE:
			throw ParserException("aggregate function calls cannot be nested");
		case ExpressionClass::WINDOW:
			throw ParserException("aggregate function calls cannot contain window function calls");
		case ExpressionClass::SUBQUERY:
			return BindSubqueryExpression(move(expr), depth);
		case ExpressionClass::COLUMN_REF: {
			// we are inside an aggregation, use the normal TableBinder to bind the column
			auto result = BindColumnRefExpression(move(expr), depth);
			if (!result.HasError()) {
				// column was successfully bound
				bound_columns = true;
			}
			return result;
		}
		case ExpressionClass::FUNCTION:
			return BindFunctionExpression(move(expr), depth);
		default:
			return BindChildren(move(expr), depth);
	}
}
