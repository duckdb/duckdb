#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

void ExpressionBinder::CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children,
                                               vector<SQLType> &types) {
	for (index_t i = 0; i < types.size(); i++) {
		auto target_type = i < function.arguments.size() ? function.arguments[i] : function.varargs;
		if (target_type.id != SQLTypeId::ANY && types[i] != target_type) {
			// type of child does not match type of function argument: add a cast
			children[i] = AddCastToType(move(children[i]), types[i], target_type);
		}
	}
}

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, index_t depth) {
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

BindResult ExpressionBinder::BindFunction(FunctionExpression &function, ScalarFunctionCatalogEntry *func,
                                          index_t depth) {
	// bind the children of the function expression
	string error;
	for (index_t i = 0; i < function.children.size(); i++) {
		BindChild(function.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully
	// extract the children and types
	vector<SQLType> types;
	vector<unique_ptr<Expression>> children;
	for (index_t i = 0; i < function.children.size(); i++) {
		auto &child = (BoundExpression &)*function.children[i];
		types.push_back(child.sql_type);
		children.push_back(move(child.expr));
	}
	// bind the function
	index_t best_function = Function::BindFunction(func->name, func->functions, types);
	// found a matching function!
	auto &bound_function = func->functions[best_function];
	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children, types);

	// types match up, get the result type
	auto return_type = bound_function.return_type;
	// now create the function
	auto result =
	    make_unique<BoundFunctionExpression>(GetInternalType(return_type), bound_function, function.is_operator);
	result->children = move(children);
	if (bound_function.bind) {
		result->bind_info = bound_function.bind(*result, context);
	}
	return BindResult(move(result), return_type);
}

BindResult ExpressionBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry *function,
                                           index_t depth) {
	return BindResult(UnsupportedAggregateMessage());
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}
