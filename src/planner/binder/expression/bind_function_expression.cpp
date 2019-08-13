#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/function_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression_binder.hpp"
#include "common/cast_rules.hpp"

using namespace duckdb;
using namespace std;

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

static index_t BindFunctionFromArguments(vector<ScalarFunction> &functions, vector<SQLType> &arguments) {
	index_t best_function = INVALID_INDEX;
	int64_t best_score = numeric_limits<int64_t>::max();
	for(index_t f_idx = 0; f_idx < functions.size(); f_idx++) {
		auto &func = functions[f_idx];
		if (func.arguments.size() != arguments.size()) {
			// invalid argument count: check the next function
			continue;
		}
		// check the arguments of the function
		int64_t score = 0;
		for(index_t i = 0; i < arguments.size(); i++) {
			if (arguments[i] == func.arguments[i]) {
				// arguments match: do nothing
				continue;
			}
			if (CastRules::ImplicitCast(arguments[i], func.arguments[i])) {
				// we can implicitly cast, add one to the amount of required casts
				score++;
			} else {
				// we can't implicitly cast: throw an error
				score = -1;
				break;
			}
		}
		if (score < 0) {
			// auto casting was not possible
			continue;
		}
		if (score >= best_score) {
			continue;
		}
		best_score = score;
		best_function = f_idx;
	}
	return best_function;
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
	index_t best_function = BindFunctionFromArguments(func->functions, types);
	if (best_function == INVALID_INDEX) {
		// no matching function was found, throw an error
		string call_str = Function::CallToString(func->name, types);
		string candidate_str = "";
		for(auto &f : func->functions) {
			candidate_str += "\t" + Function::CallToString(f.name, f.arguments, f.return_type) + "\n";
		}
		throw BinderException("No function matches the given name and argument types '%s'. You might need to add explicit type casts.\n\tCandidate functions:\n%s", call_str.c_str(), candidate_str.c_str());
	}
	// found a matching function!
	auto &bound_function = func->functions[best_function];
	// check if we need to add casts to the children
	for(index_t i = 0; i < types.size(); i++) {
		auto target_type = bound_function.arguments[i];
		if (types[i] != target_type) {
			// type of child does not match type of function argument: add a cast
			children[i] = make_unique<BoundCastExpression>(GetInternalType(target_type), move(children[i]), types[i], target_type);
		}
	}

	// types match up, get the result type
	auto return_type = bound_function.return_type;
	// now create the function
	auto result = make_unique<BoundFunctionExpression>(GetInternalType(return_type), bound_function);
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
