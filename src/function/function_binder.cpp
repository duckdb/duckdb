#include "duckdb/function/function_binder.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

FunctionBinder::FunctionBinder(ClientContext &context) : context(context) {
}

optional_idx FunctionBinder::BindVarArgsFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments) {
	if (arguments.size() < func.arguments.size()) {
		// not enough arguments to fulfill the non-vararg part of the function
		return optional_idx();
	}
	idx_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		LogicalType arg_type = i < func.arguments.size() ? func.arguments[i] : func.varargs;
		if (arguments[i] == arg_type) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastFunctionSet::Get(context).ImplicitCastCost(arguments[i], arg_type);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += idx_t(cast_cost);
		} else {
			// we can't implicitly cast: throw an error
			return optional_idx();
		}
	}
	return cost;
}

optional_idx FunctionBinder::BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments) {
	if (func.HasVarArgs()) {
		// special case varargs function
		return BindVarArgsFunctionCost(func, arguments);
	}
	if (func.arguments.size() != arguments.size()) {
		// invalid argument count: check the next function
		return optional_idx();
	}
	idx_t cost = 0;
	bool has_parameter = false;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i].id() == LogicalTypeId::UNKNOWN) {
			has_parameter = true;
			continue;
		}
		int64_t cast_cost = CastFunctionSet::Get(context).ImplicitCastCost(arguments[i], func.arguments[i]);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += idx_t(cast_cost);
		} else {
			// we can't implicitly cast: throw an error
			return optional_idx();
		}
	}
	if (has_parameter) {
		// all arguments are implicitly castable and there is a parameter - return 0 as cost
		return 0;
	}
	return cost;
}

template <class T>
vector<idx_t> FunctionBinder::BindFunctionsFromArguments(const string &name, FunctionSet<T> &functions,
                                                         const vector<LogicalType> &arguments, ErrorData &error) {
	optional_idx best_function;
	idx_t lowest_cost = NumericLimits<idx_t>::Maximum();
	vector<idx_t> candidate_functions;
	for (idx_t f_idx = 0; f_idx < functions.functions.size(); f_idx++) {
		auto &func = functions.functions[f_idx];
		// check the arguments of the function
		auto bind_cost = BindFunctionCost(func, arguments);
		if (!bind_cost.IsValid()) {
			// auto casting was not possible
			continue;
		}
		auto cost = bind_cost.GetIndex();
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
	if (!best_function.IsValid()) {
		// no matching function was found, throw an error
		vector<string> candidates;
		for (auto &f : functions.functions) {
			candidates.push_back(f.ToString());
		}
		error = ErrorData(BinderException::NoMatchingFunction(name, arguments, candidates));
		return candidate_functions;
	}
	candidate_functions.push_back(best_function.GetIndex());
	return candidate_functions;
}

template <class T>
optional_idx FunctionBinder::MultipleCandidateException(const string &name, FunctionSet<T> &functions,
                                                        vector<idx_t> &candidate_functions,
                                                        const vector<LogicalType> &arguments, ErrorData &error) {
	D_ASSERT(functions.functions.size() > 1);
	// there are multiple possible function definitions
	// throw an exception explaining which overloads are there
	string call_str = Function::CallToString(name, arguments);
	string candidate_str;
	for (auto &conf : candidate_functions) {
		T f = functions.GetFunctionByOffset(conf);
		candidate_str += "\t" + f.ToString() + "\n";
	}
	error = ErrorData(
	    ExceptionType::BINDER,
	    StringUtil::Format("Could not choose a best candidate function for the function call \"%s\". In order to "
	                       "select one, please add explicit type casts.\n\tCandidate functions:\n%s",
	                       call_str, candidate_str));
	return optional_idx();
}

template <class T>
optional_idx FunctionBinder::BindFunctionFromArguments(const string &name, FunctionSet<T> &functions,
                                                       const vector<LogicalType> &arguments, ErrorData &error) {
	auto candidate_functions = BindFunctionsFromArguments<T>(name, functions, arguments, error);
	if (candidate_functions.empty()) {
		// no candidates
		return optional_idx();
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

optional_idx FunctionBinder::BindFunction(const string &name, ScalarFunctionSet &functions,
                                          const vector<LogicalType> &arguments, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, AggregateFunctionSet &functions,
                                          const vector<LogicalType> &arguments, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, TableFunctionSet &functions,
                                          const vector<LogicalType> &arguments, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, PragmaFunctionSet &functions, vector<Value> &parameters,
                                          ErrorData &error) {
	vector<LogicalType> types;
	for (auto &value : parameters) {
		types.push_back(value.type());
	}
	auto entry = BindFunctionFromArguments(name, functions, types, error);
	if (!entry.IsValid()) {
		error.Throw();
	}
	auto candidate_function = functions.GetFunctionByOffset(entry.GetIndex());
	// cast the input parameters
	for (idx_t i = 0; i < parameters.size(); i++) {
		auto target_type =
		    i < candidate_function.arguments.size() ? candidate_function.arguments[i] : candidate_function.varargs;
		parameters[i] = parameters[i].CastAs(context, target_type);
	}
	return entry;
}

vector<LogicalType> FunctionBinder::GetLogicalTypesFromExpressions(vector<unique_ptr<Expression>> &arguments) {
	vector<LogicalType> types;
	types.reserve(arguments.size());
	for (auto &argument : arguments) {
		types.push_back(ExpressionBinder::GetExpressionReturnType(*argument));
	}
	return types;
}

optional_idx FunctionBinder::BindFunction(const string &name, ScalarFunctionSet &functions,
                                          vector<unique_ptr<Expression>> &arguments, ErrorData &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, AggregateFunctionSet &functions,
                                          vector<unique_ptr<Expression>> &arguments, ErrorData &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

optional_idx FunctionBinder::BindFunction(const string &name, TableFunctionSet &functions,
                                          vector<unique_ptr<Expression>> &arguments, ErrorData &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return BindFunction(name, functions, types, error);
}

enum class LogicalTypeComparisonResult : uint8_t { IDENTICAL_TYPE, TARGET_IS_ANY, DIFFERENT_TYPES };

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
	if (source_type.id() == LogicalTypeId::ARRAY && target_type.id() == LogicalTypeId::ARRAY) {
		return RequiresCast(ArrayType::GetChildType(source_type), ArrayType::GetChildType(target_type));
	}
	return LogicalTypeComparisonResult::DIFFERENT_TYPES;
}

bool TypeRequiresPrepare(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return true;
	}
	if (type.id() == LogicalTypeId::LIST) {
		return TypeRequiresPrepare(ListType::GetChildType(type));
	}
	return false;
}

LogicalType PrepareTypeForCastRecursive(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return AnyType::GetTargetType(type);
	}
	if (type.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(PrepareTypeForCastRecursive(ListType::GetChildType(type)));
	}
	return type;
}

void PrepareTypeForCast(LogicalType &type) {
	if (!TypeRequiresPrepare(type)) {
		return;
	}
	type = PrepareTypeForCastRecursive(type);
}

void FunctionBinder::CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children) {
	for (auto &arg : function.arguments) {
		PrepareTypeForCast(arg);
	}
	PrepareTypeForCast(function.varargs);

	for (idx_t i = 0; i < children.size(); i++) {
		auto target_type = i < function.arguments.size() ? function.arguments[i] : function.varargs;
		if (target_type.id() == LogicalTypeId::STRING_LITERAL || target_type.id() == LogicalTypeId::INTEGER_LITERAL) {
			throw InternalException(
			    "Function %s returned a STRING_LITERAL or INTEGER_LITERAL type - return an explicit type instead",
			    function.name);
		}
		target_type.Verify();
		// don't cast lambda children, they get removed before execution
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
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	// bind the function
	auto &function =
	    Catalog::GetSystemCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, schema, name);
	D_ASSERT(function.type == CatalogType::SCALAR_FUNCTION_ENTRY);
	return BindScalarFunction(function.Cast<ScalarFunctionCatalogEntry>(), std::move(children), error, is_operator,
	                          binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(ScalarFunctionCatalogEntry &func,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	// bind the function
	auto best_function = BindFunction(func.name, func.functions, children, error);
	if (!best_function.IsValid()) {
		return nullptr;
	}

	// found a matching function!
	auto bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());

	// If any of the parameters are NULL, the function will just be replaced with a NULL constant
	// But this NULL constant needs to have to correct type, because we use LogicalType::SQLNULL for binding macro's
	// However, some functions may have an invalid return type, so we default to SQLNULL for those
	LogicalType return_type_if_null;
	switch (bound_function.return_type.id()) {
	case LogicalTypeId::ANY:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
	case LogicalTypeId::ARRAY:
		return_type_if_null = LogicalType::SQLNULL;
		break;
	default:
		return_type_if_null = bound_function.return_type;
	}

	if (bound_function.null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		for (auto &child : children) {
			if (child->return_type == LogicalTypeId::SQLNULL) {
				return make_uniq<BoundConstantExpression>(Value(return_type_if_null));
			}
			if (!child->IsFoldable()) {
				continue;
			}
			Value result;
			if (!ExpressionExecutor::TryEvaluateScalar(context, *child, result)) {
				continue;
			}
			if (result.IsNull()) {
				return make_uniq<BoundConstantExpression>(Value(return_type_if_null));
			}
		}
	}
	return BindScalarFunction(bound_function, std::move(children), is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(ScalarFunction bound_function,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	unique_ptr<FunctionData> bind_info;
	if (bound_function.bind) {
		bind_info = bound_function.bind(context, bound_function, children);
	}
	if (bound_function.get_modified_databases && binder) {
		auto &properties = binder->GetStatementProperties();
		FunctionModifiedDatabasesInput input(bind_info, properties.modified_databases);
		bound_function.get_modified_databases(input);
	}
	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children);

	// now create the function
	auto return_type = bound_function.return_type;
	unique_ptr<Expression> result;
	auto result_func = make_uniq<BoundFunctionExpression>(std::move(return_type), std::move(bound_function),
	                                                      std::move(children), std::move(bind_info), is_operator);
	if (result_func->function.bind_expression) {
		// if a bind_expression callback is registered - call it and emit the resulting expression
		FunctionBindExpressionInput input(context, result_func->bind_info.get(), *result_func);
		result = result_func->function.bind_expression(input);
	}
	if (!result) {
		result = std::move(result_func);
	}
	return result;
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
