#include "planner/expression_binder/default_binder.hpp"

#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/parsed_data.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

DefaultBinder::DefaultBinder(Binder &binder, ClientContext &context, CreateTableInformation &info)
    : ExpressionBinder(binder, context), info(info) {
}

BindResult DefaultBinder::BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult("aggregate functions are not allowed in DEFAULT expressions");
	case ExpressionClass::COLUMN_REF:
		return BindResult("default value must be scalar expression");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in DEFAULT expression");
	case ExpressionClass::DEFAULT:
		return BindResult("DEFAULT expression cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in DEFAULT expressions");
	case ExpressionClass::FUNCTION: {
		BindResult result = ExpressionBinder::BindExpression((FunctionExpression &)expr, depth);
		if (result.HasError()) {
			return result;
		}
		auto &function = (BoundFunctionExpression &)*result.expression;
		if (function.bound_function->get_dependency) {
			auto dependency = function.bound_function->get_dependency(function);
			if (dependency) {
				throw NotImplementedException("catalog dependency in function!");
			}
		}
		return result;
	}
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}
