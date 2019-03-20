#include "planner/expression_binder.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "parser/expression/function_expression.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, uint32_t depth) {
	// bind the children of the function expression
	string error;
	for(size_t i = 0; i < function.children.size(); i++) {
		string result = Bind(&function.children[i], depth);
		if (!result.empty()) {
			error = result;
		}
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully	
	// lookup the function in the catalog
	auto func = context.db.catalog.GetScalarFunction(context.ActiveTransaction(), function.schema, function.function_name);
	// extract the children and types
	vector<SQLType> types;
	vector<unique_ptr<Expression>> children;
	for(size_t i = 0; i < function.children.size(); i++) {
		auto child = GetExpression(function.children[i]);
		types.push_back(child->sql_type);
		children.push_back(move(child));
	}
	// now check if the child types match up with the function
	if (!func->matches(types)) {
		// types do not match up, throw exception
		string type_str;
		for(size_t i = 0; i < types.size(); i++) {
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
	auto result = make_unique<BoundFunctionExpression>(GetInternalType(return_type), return_type, func);
	result->children = move(children);
	return BindResult(move(result));
}
