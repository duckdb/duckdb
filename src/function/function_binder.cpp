#include "duckdb/function/function_binder.hpp"
#include "duckdb/common/limits.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

FunctionBinder::FunctionBinder(ClientContext &context) : context(context) {
}

int64_t FunctionBinder::BindVarArgsFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments) {
	if (arguments.size() < func.arguments.size()) {
		// not enough arguments to fulfill the non-vararg part of the function
		return -1;
	}
	int64_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		LogicalType arg_type = i < func.arguments.size() ? func.arguments[i] : func.varargs;
		if (arguments[i] == arg_type) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastFunctionSet::Get(context).ImplicitCastCost(arguments[i], arg_type);
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

int64_t FunctionBinder::BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments) {
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
		int64_t cast_cost = CastFunctionSet::Get(context).ImplicitCastCost(arguments[i], func.arguments[i]);
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
vector<idx_t> FunctionBinder::BindFunctionsFromArguments(const string &name, FunctionSet<T> &functions,
                                                         const vector<LogicalType> &arguments, string &error) {
	idx_t best_function = DConstants::INVALID_INDEX;
	int64_t lowest_cost = NumericLimits<int64_t>::Maximum();
	vector<idx_t> candidate_functions;
	for (idx_t f_idx = 0; f_idx < functions.functions.size(); f_idx++) {
		auto &func = functions.functions[f_idx];
		// check the arguments of the function
		int64_t cost = BindFunctionCost(func, arguments);
		if (cost < 0) {
			// auto casting was not possible
			continue;
		}
		if (cost == lowest_cost) {
			candidate_functions.push_back(f_idx);
			continue;
		}
		if (cost > lowest_cost) {
			continue;
		}
		candidate_functions.clear();
		lowest_cost = cost;
		best_function = f_idx;
	}
	if (best_function == DConstants::INVALID_INDEX) {
		// no matching function was found, throw an error
		string call_str = Function::CallToString(name, arguments);
		string candidate_str = "";
		for (auto &f : functions.functions) {
			candidate_str += "\t" + f.ToString() + "\n";
		}
		error = StringUtil::Format("No function matches the given name and argument types '%s'. You might need to add "
		                           "explicit type casts.\n\tCandidate functions:\n%s",
		                           call_str, candidate_str);
		return candidate_functions;
	}
	candidate_functions.push_back(best_function);
	return candidate_functions;
}

template <class T>
idx_t FunctionBinder::MultipleCandidateException(const string &name, FunctionSet<T> &functions,
                                                 vector<idx_t> &candidate_functions,
                                                 const vector<LogicalType> &arguments, string &error) {
	D_ASSERT(functions.functions.size() > 1);
	// there are multiple possible function definitions
	// throw an exception explaining which overloads are there
	string call_str = Function::CallToString(name, arguments);
	string candidate_str = "";
	for (auto &conf : candidate_functions) {
		T f = functions.GetFunctionByOffset(conf);
		candidate_str += "\t" + f.ToString() + "\n";
	}
	error = StringUtil::Format("Could not choose a best candidate function for the function call \"%s\". In order to "
	                           "select one, please add explicit type casts.\n\tCandidate functions:\n%s",
	                           call_str, candidate_str);
	return DConstants::INVALID_INDEX;
}

template <class T>
idx_t FunctionBinder::BindFunctionFromArguments(const string &name, FunctionSet<T> &functions,
                                                const vector<LogicalType> &arguments, string &error) {
	auto candidate_functions = BindFunctionsFromArguments<T>(name, functions, arguments, error);
	if (candidate_functions.empty()) {
		// no candidates
		return DConstants::INVALID_INDEX;
	}
	if (candidate_functions.size() > 1) {
		// multiple candidates, check if there are any unknown arguments
		bool has_parameters = false;
		for (auto &arg_type : arguments) {
			if (arg_type.id() == LogicalTypeId::UNKNOWN) {
				//! there are! we could not resolve parameters in this case
				throw ParameterNotResolvedException();
			}
		}
		if (!has_parameters) {
			return MultipleCandidateException(name, functions, candidate_functions, arguments, error);
		}
	}
	return candidate_functions[0];
}

idx_t FunctionBinder::BindFunction(const string &name, ScalarFunctionSet &functions,
                                   const vector<LogicalType> &arguments, string &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

idx_t FunctionBinder::BindFunction(const string &name, AggregateFunctionSet &functions,
                                   const vector<LogicalType> &arguments, string &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

idx_t FunctionBinder::BindFunction(const string &name, TableFunctionSet &functions,
                                   const vector<LogicalType> &arguments, string &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

idx_t FunctionBinder::BindFunction(const string &name, PragmaFunctionSet &functions, PragmaInfo &info, string &error) {
	vector<LogicalType> types;
	for (auto &value : info.parameters) {
		types.push_back(value.type());
	}
	idx_t entry = BindFunctionFromArguments(name, functions, types, error);
	if (entry == DConstants::INVALID_INDEX) {
		throw BinderException(error);
	}
	auto candidate_function = functions.GetFunctionByOffset(entry);
	// cast the input parameters
	for (idx_t i = 0; i < info.parameters.size(); i++) {
		auto target_type =
		    i < candidate_function.arguments.size() ? candidate_function.arguments[i] : candidate_function.varargs;
		info.parameters[i] = info.parameters[i].CastAs(context, target_type);
	}
	return entry;
}

vector<LogicalType> FunctionBinder::GetLogicalTypesFromExpressions(vector<unique_ptr<Expression>> &arguments) {
	vector<LogicalType> types;
	types.reserve(arguments.size());
	for (auto &argument : arguments) {
		types.push_back(argument->return_type);
	}
	return types;
}

idx_t FunctionBinder::BindFunction(const string &name, ScalarFunctionSet &functions,
                                   vector<unique_ptr<Expression>> &arguments, string &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

idx_t FunctionBinder::BindFunction(const string &name, AggregateFunctionSet &functions,
                                   vector<unique_ptr<Expression>> &arguments, string &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

idx_t FunctionBinder::BindFunction(const string &name, TableFunctionSet &functions,
                                   vector<unique_ptr<Expression>> &arguments, string &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

enum class LogicalTypeComparisonResult { IDENTICAL_TYPE, TARGET_IS_ANY, DIFFERENT_TYPES };

LogicalTypeComparisonResult RequiresCast(const LogicalType &source_type, const LogicalType &target_type) {
	if (target_type.id() == LogicalTypeId::ANY) {
		return LogicalTypeComparisonResult::TARGET_IS_ANY;
	}
	if (source_type == target_type) {
		return LogicalTypeComparisonResult::IDENTICAL_TYPE;
	}
	if (source_type.id() == LogicalTypeId::LIST && target_type.id() == LogicalTypeId::LIST) {
		return RequiresCast(ListType::GetChildType(source_type), ListType::GetChildType(target_type));
	}
	return LogicalTypeComparisonResult::DIFFERENT_TYPES;
}

void FunctionBinder::CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children) {
	for (idx_t i = 0; i < children.size(); i++) {
		auto target_type = i < function.arguments.size() ? function.arguments[i] : function.varargs;
		target_type.Verify();
		// don't cast lambda children, they get removed anyways
		if (children[i]->return_type.id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		// check if the type of child matches the type of function argument
		// if not we need to add a cast
		auto cast_result = RequiresCast(children[i]->return_type, target_type);
		// except for one special case: if the function accepts ANY argument
		// in that case we don't add a cast
		if (cast_result == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			children[i] = BoundCastExpression::AddCastToType(context, std::move(children[i]), target_type);
		}
	}
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const string &schema, const string &name,
                                                          vector<unique_ptr<Expression>> children, string &error,
                                                          bool is_operator, Binder *binder) {
	// bind the function
	auto &function =
	    Catalog::GetSystemCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, schema, name);
	D_ASSERT(function.type == CatalogType::SCALAR_FUNCTION_ENTRY);
	return BindScalarFunction(function.Cast<ScalarFunctionCatalogEntry>(), std::move(children), error, is_operator,
	                          binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(ScalarFunctionCatalogEntry &func,
                                                          vector<unique_ptr<Expression>> children, string &error,
                                                          bool is_operator, Binder *binder) {
	// bind the function
	idx_t best_function = BindFunction(func.name, func.functions, children, error);
	if (best_function == DConstants::INVALID_INDEX) {
		return nullptr;
	}

	// found a matching function!
	auto bound_function = func.functions.GetFunctionByOffset(best_function);

	if (bound_function.null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		for (auto &child : children) {
			if (child->return_type == LogicalTypeId::SQLNULL) {
				return make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL));
			}
		}
	}
	return BindScalarFunction(bound_function, std::move(children), is_operator);
}

unique_ptr<BoundFunctionExpression> FunctionBinder::BindScalarFunction(ScalarFunction bound_function,
                                                                       vector<unique_ptr<Expression>> children,
                                                                       bool is_operator) {
	unique_ptr<FunctionData> bind_info;
	if (bound_function.bind) {
		bind_info = bound_function.bind(context, bound_function, children);
	}
	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children);

	// now create the function
	auto return_type = bound_function.return_type;
	return make_uniq<BoundFunctionExpression>(std::move(return_type), std::move(bound_function), std::move(children),
	                                          std::move(bind_info), is_operator);
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(AggregateFunction bound_function,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	unique_ptr<FunctionData> bind_info;
	if (bound_function.bind) {
		bind_info = bound_function.bind(context, bound_function, children);
		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.arguments.size(), children.size()));
	}

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children);

	return make_uniq<BoundAggregateExpression>(std::move(bound_function), std::move(children), std::move(filter),
	                                           std::move(bind_info), aggr_type);
}

} // namespace duckdb
