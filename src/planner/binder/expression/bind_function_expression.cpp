#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, idx_t depth) {
	// lookup the function in the catalog
	auto func = context.catalog.GetFunction(context.ActiveTransaction(), function.schema, function.function_name);
	if (func->type == CatalogType::SCALAR_FUNCTION) {
		// scalar function
		return BindFunction(function, (ScalarFunctionCatalogEntry *)func, depth);
	} else {
		// aggregate function
		return BindAggregate(function, (AggregateFunctionCatalogEntry *)func, depth);
	}
}

BindResult ExpressionBinder::BindFunction(FunctionExpression &function, ScalarFunctionCatalogEntry *func, idx_t depth) {
	// bind the children of the function expression
	string error;
	for (idx_t i = 0; i < function.children.size(); i++) {
		BindChild(function.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully
	// extract the children and types
	vector<SQLType> arguments;
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < function.children.size(); i++) {
		auto &child = (BoundExpression &)*function.children[i];
		arguments.push_back(child.sql_type);
		children.push_back(move(child.expr));
	}

	auto result = ScalarFunction::BindScalarFunction(context, *func, arguments, move(children), function.is_operator);
	auto sql_return_type = result->sql_return_type;
	return BindResult(move(result), sql_return_type);
}

BindResult ExpressionBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry *function,
                                           idx_t depth) {
	return BindResult(UnsupportedAggregateMessage());
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}
