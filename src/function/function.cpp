#include "duckdb/function/function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

// add your initializer for new functions here
void BuiltinFunctions::Initialize() {
	RegisterSQLiteFunctions();
	RegisterReadFunctions();

	RegisterAlgebraicAggregates();
	RegisterDistributiveAggregates();
	RegisterNestedAggregates();

	RegisterDateFunctions();
	RegisterMathFunctions();
	RegisterOperators();
	RegisterSequenceFunctions();
	RegisterStringFunctions();
	RegisterNestedFunctions();
	RegisterTrigonometricsFunctions();

	// initialize collations
	AddCollation("nocase", LowerFun::GetFunction(), true);
	AddCollation("noaccent", StripAccentsFun::GetFunction());
}

BuiltinFunctions::BuiltinFunctions(ClientContext &context, Catalog &catalog) : context(context), catalog(catalog) {
}

void BuiltinFunctions::AddCollation(string name, ScalarFunction function, bool combinable,
                                    bool not_required_for_equality) {
	CreateCollationInfo info(move(name), move(function), combinable, not_required_for_equality);
	catalog.CreateCollation(context, &info);
}

void BuiltinFunctions::AddFunction(AggregateFunctionSet set) {
	CreateAggregateFunctionInfo info(set);
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(AggregateFunction function) {
	CreateAggregateFunctionInfo info(function);
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(ScalarFunction function) {
	CreateScalarFunctionInfo info(function);
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(vector<string> names, ScalarFunction function) {
	for (auto &name : names) {
		function.name = name;
		AddFunction(function);
	}
}

void BuiltinFunctions::AddFunction(ScalarFunctionSet set) {
	CreateScalarFunctionInfo info(set);
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(TableFunction function) {
	CreateTableFunctionInfo info(function);
	catalog.CreateTableFunction(context, &info);
}

string Function::CallToString(string name, vector<SQLType> arguments) {
	string result = name + "(";
	result += StringUtil::Join(arguments, arguments.size(), ", ",
	                           [](const SQLType &argument) { return SQLTypeToString(argument); });
	return result + ")";
}

string Function::CallToString(string name, vector<SQLType> arguments, SQLType return_type) {
	string result = CallToString(name, arguments);
	result += " -> " + SQLTypeToString(return_type);
	return result;
}

static int64_t BindVarArgsFunctionCost(SimpleFunction &func, vector<SQLType> &arguments) {
	if (arguments.size() < func.arguments.size()) {
		// not enough arguments to fulfill the non-vararg part of the function
		return -1;
	}
	int64_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		SQLType arg_type = i < func.arguments.size() ? func.arguments[i] : func.varargs;
		if (arguments[i] == arg_type) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastRules::ImplicitCast(arguments[i], arg_type);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += cast_cost;
		} else {
			// we can't implicitly cast: throw an error
			return -1;
		}
	}
	return cost;
}

static int64_t BindFunctionCost(SimpleFunction &func, vector<SQLType> &arguments) {
	if (func.HasVarArgs()) {
		// special case varargs function
		return BindVarArgsFunctionCost(func, arguments);
	}
	if (func.arguments.size() != arguments.size()) {
		// invalid argument count: check the next function
		return -1;
	}
	int64_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i] == func.arguments[i]) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastRules::ImplicitCast(arguments[i], func.arguments[i]);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += cast_cost;
		} else {
			// we can't implicitly cast: throw an error
			return -1;
		}
	}
	return cost;
}

template <class T>
static idx_t BindFunctionFromArguments(string name, vector<T> &functions, vector<SQLType> &arguments) {
	idx_t best_function = INVALID_INDEX;
	int64_t lowest_cost = numeric_limits<int64_t>::max();
	vector<idx_t> conflicting_functions;
	for (idx_t f_idx = 0; f_idx < functions.size(); f_idx++) {
		auto &func = functions[f_idx];
		// check the arguments of the function
		int64_t cost = BindFunctionCost(func, arguments);
		if (cost < 0) {
			// auto casting was not possible
			continue;
		}
		if (cost == lowest_cost) {
			conflicting_functions.push_back(f_idx);
			continue;
		}
		if (cost > lowest_cost) {
			continue;
		}
		conflicting_functions.clear();
		lowest_cost = cost;
		best_function = f_idx;
	}
	if (conflicting_functions.size() > 0) {
		// there are multiple possible function definitions
		// throw an exception explaining which overloads are there
		conflicting_functions.push_back(best_function);
		string call_str = Function::CallToString(name, arguments);
		string candidate_str = "";
		for (auto &conf : conflicting_functions) {
			auto &f = functions[conf];
			candidate_str += "\t" + f.ToString() + "\n";
		}
		throw BinderException("Could not choose a best candidate function for the function call \"%s\". In order to "
		                      "select one, please add explicit type casts.\n\tCandidate functions:\n%s",
		                      call_str.c_str(), candidate_str.c_str());
	}
	if (best_function == INVALID_INDEX) {
		// no matching function was found, throw an error
		string call_str = Function::CallToString(name, arguments);
		string candidate_str = "";
		for (auto &f : functions) {
			candidate_str += "\t" + f.ToString() + "\n";
		}
		throw BinderException("No function matches the given name and argument types '%s'. You might need to add "
		                      "explicit type casts.\n\tCandidate functions:\n%s",
		                      call_str.c_str(), candidate_str.c_str());
	}
	return best_function;
}

idx_t Function::BindFunction(string name, vector<ScalarFunction> &functions, vector<SQLType> &arguments) {
	return BindFunctionFromArguments(name, functions, arguments);
}

idx_t Function::BindFunction(string name, vector<AggregateFunction> &functions, vector<SQLType> &arguments) {
	return BindFunctionFromArguments(name, functions, arguments);
}

void SimpleFunction::CastToFunctionArguments(vector<unique_ptr<Expression>> &children, vector<SQLType> &types) {
	for (idx_t i = 0; i < types.size(); i++) {
		auto target_type = i < this->arguments.size() ? this->arguments[i] : this->varargs;
		if (target_type.id != SQLTypeId::ANY && types[i] != target_type) {
			// type of child does not match type of function argument: add a cast
			children[i] = BoundCastExpression::AddCastToType(move(children[i]), types[i], target_type);
		}
	}
}

unique_ptr<BoundFunctionExpression> ScalarFunction::BindScalarFunction(ClientContext &context, string schema,
                                                                       string name, vector<SQLType> &arguments,
                                                                       vector<unique_ptr<Expression>> children,
                                                                       bool is_operator) {
	// bind the function
	auto function = Catalog::GetCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION, schema, name);
	assert(function && function->type == CatalogType::SCALAR_FUNCTION);
	return ScalarFunction::BindScalarFunction(context, (ScalarFunctionCatalogEntry &)*function, arguments,
	                                          move(children), is_operator);
}

unique_ptr<BoundFunctionExpression>
ScalarFunction::BindScalarFunction(ClientContext &context, ScalarFunctionCatalogEntry &func, vector<SQLType> &arguments,
                                   vector<unique_ptr<Expression>> children, bool is_operator) {
	// bind the function
	idx_t best_function = Function::BindFunction(func.name, func.functions, arguments);
	// found a matching function!
	auto &bound_function = func.functions[best_function];
	// check if we need to add casts to the children
	bound_function.CastToFunctionArguments(children, arguments);

	// now create the function
	auto result =
	    make_unique<BoundFunctionExpression>(GetInternalType(bound_function.return_type), bound_function, is_operator);
	result->children = move(children);
	result->arguments = arguments;
	result->sql_return_type = bound_function.return_type;
	if (bound_function.bind) {
		result->bind_info = bound_function.bind(*result, context);
	}
	return result;
}
