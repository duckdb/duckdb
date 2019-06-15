#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/function_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, index_t depth) {
	// bind the children of the function expression
	string error;
	for (index_t i = 0; i < function.children.size(); i++) {
		BindChild(function.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully
	// lookup the function in the catalog
	auto func = context.catalog.GetScalarFunction(context.ActiveTransaction(), function.schema, function.function_name);
	// extract the children and types
	vector<SQLType> types;
	vector<unique_ptr<Expression>> children;
	for (index_t i = 0; i < function.children.size(); i++) {
		auto &child = (BoundExpression &)*function.children[i];
		types.push_back(child.sql_type);
		children.push_back(move(child.expr));
	}
	// now check if the child types match up with the function
	if (!func->matches(types)) {
		// types do not match up, throw exception
		string type_str;
		for (index_t i = 0; i < types.size(); i++) {
			if (i > 0) {
				type_str += ", ";
			}
			type_str += SQLTypeToString(types[i]);
		}
		throw BinderException("Unsupported input types for function %s(%s)", func->name.c_str(), type_str.c_str());
	}
	// types match up, get the result type
	auto return_type = func->return_type(types);
	// now create the function
	auto result = make_unique<BoundFunctionExpression>(GetInternalType(return_type), func);
	result->children = move(children);
	if (func->bind) {
		result->bind_info = func->bind(*result, context);
	}
	return BindResult(move(result), return_type);
}
