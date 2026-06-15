#include "duckdb/function/function_binder.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

FunctionBinder::FunctionBinder(ClientContext &context_p) : binder(nullptr), context(context_p) {
}
FunctionBinder::FunctionBinder(Binder &binder_p) : binder(&binder_p), context(binder_p.context) {
}

// Split the full (maybe-named) argument list into positional types + named (name, type) pairs.
// Returns false if a positional argument follows a named one, which is not allowed.
static bool TrySplitArgumentTypes(const vector<pair<Identifier, unique_ptr<Expression>>> &arguments,
                                  vector<LogicalType> &positional, vector<pair<Identifier, LogicalType>> &named) {
	for (auto &arg : arguments) {
		auto type = ExpressionBinder::GetExpressionReturnType(*arg.second);
		if (!arg.first.empty()) {
			named.emplace_back(arg.first, std::move(type));
			continue;
		}
		if (named.empty()) {
			positional.push_back(std::move(type));
			continue;
		}
		// a positional argument cannot follow a named one
		return false;
	}
	return true;
}

// Split the full (maybe-named) argument list into the positional + named (keyword) children.
static auto SplitArguments(vector<pair<Identifier, unique_ptr<Expression>>> arguments)
    -> pair<vector<unique_ptr<Expression>>, vector<pair<Identifier, unique_ptr<Expression>>>> {
	vector<unique_ptr<Expression>> regular_args;
	vector<pair<Identifier, unique_ptr<Expression>>> keyword_args;

	for (auto &arg : arguments) {
		if (!arg.first.empty()) {
			keyword_args.push_back(std::move(arg));
			continue;
		}
		if (keyword_args.empty()) {
			regular_args.push_back(std::move(arg.second));
			continue;
		}
		// Defensive: a positional argument following a named one should already have been rejected during binding
		// (see TrySplitArgumentTypes / BindFunctionWithImplicitNaming).
		throw BinderException(arg.second->GetQueryLocation(),
		                      "Positional argument '%s' cannot follow named arguments in a function call.",
		                      arg.second->ToString());
	}

	return {std::move(regular_args), std::move(keyword_args)};
}

optional_idx FunctionBinder::BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments,
                                              const vector<pair<Identifier, LogicalType>> &named_arguments) {
	const auto &sig = func.GetSignature();

	// Compute total number of arguments passed
	const auto received_arg_count = static_cast<idx_t>(arguments.size() + named_arguments.size());

	// And the minimum and maximum number of arguments the function can accept
	const auto minimum_arg_count = sig.GetRequiredParameterCount();

	const auto maximum_arg_count = sig.HasVarArgs() ? NumericLimits<idx_t>::Maximum() : sig.GetParameterCount();

	if (received_arg_count < minimum_arg_count) {
		// We have fewer arguments than the function requires, so this function cannot be a match.
		return optional_idx();
	}

	if (received_arg_count > maximum_arg_count) {
		// We have more arguments than the function can take, so this function cannot be a match.
		return optional_idx();
	}

	idx_t cost = 0;
	bool has_parameter = false;

	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i].id() == LogicalTypeId::UNKNOWN) {
			has_parameter = true;
			continue;
		}

		auto arg_type = i < sig.GetParameterCount() ? sig.GetParameter(i).GetType() : sig.GetVarArgs();

		int64_t cast_cost = CastFunctionSet::ImplicitCastCost(context, arguments[i], arg_type);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += idx_t(cast_cost);
		} else {
			// we can't implicitly cast: throw an error
			return optional_idx();
		}
	}

	// Now check the named arguments
	for (idx_t i = 0; i < named_arguments.size(); i++) {
		auto &named_arg = named_arguments[i];
		auto opt_param_idx = sig.GetParameterIndexByName(named_arg.first);

		if (!opt_param_idx.IsValid()) {
			if (!sig.HasVarArgs()) {
				// no parameter with this name: continue
				return optional_idx();
			}

			// This is a named vararg argument, we can skip the parameter index check as varargs are always at the end
			// of the argument list
			auto &vararg_type = sig.GetVarArgs();
			int64_t cast_cost = CastFunctionSet::ImplicitCastCost(context, named_arg.second, vararg_type);
			if (cast_cost >= 0) {
				// we can implicitly cast, add the cost to the total cost
				cost += static_cast<idx_t>(cast_cost);
			} else {
				// we can't implicitly cast: throw an error
				return optional_idx();
			}
		} else {
			// This parameter index might technically be invalid, in that it may point to a parameter that has already
			// been filled by a positional argument. However, we will catch that later when we actually bind the
			// argument expressions to construct the bound function expression. At that point we will also have more
			// context so we can give a better error message.
			const auto param_idx = opt_param_idx.GetIndex();

			if (param_idx < arguments.size()) {
				// If already covered by a positional argument, skip the cost check here.
				continue;
			}

			auto &param = sig.GetParameter(param_idx);
			int64_t cast_cost = CastFunctionSet::ImplicitCastCost(context, named_arg.second, param.GetType());
			if (cast_cost >= 0) {
				cost += idx_t(cast_cost);
			} else {
				return optional_idx();
			}
		}
	}

	if (has_parameter) {
		// all arguments are implicitly castable and there is a parameter - return 0 as cost
		return 0;
	}
	return cost;
}

optional_idx FunctionBinder::BindVarArgsFunctionCost(const SimpleNamedParameterFunction &func,
                                                     const vector<LogicalType> &arguments) {
	if (arguments.size() < func.GetArguments().size()) {
		// not enough arguments to fulfill the non-vararg part of the function
		return optional_idx();
	}
	idx_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		LogicalType arg_type = i < func.GetArguments().size() ? func.GetArguments()[i] : func.GetVarArgs();
		if (arguments[i] == arg_type) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastFunctionSet::ImplicitCastCost(context, arguments[i], arg_type);
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

optional_idx FunctionBinder::BindFunctionCost(const SimpleNamedParameterFunction &func,
                                              const vector<LogicalType> &arguments,
                                              const vector<pair<Identifier, LogicalType>> &) {
	if (func.HasVarArgs()) {
		// special case varargs function
		return BindVarArgsFunctionCost(func, arguments);
	}
	if (func.GetArguments().size() != arguments.size()) {
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
		int64_t cast_cost = CastFunctionSet::ImplicitCastCost(context, arguments[i], func.GetArguments()[i]);
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
vector<idx_t> FunctionBinder::BindFunctionsFromArguments(const Identifier &name, const FunctionSet<T> &functions,
                                                         const vector<LogicalType> &arguments,
                                                         const vector<pair<Identifier, LogicalType>> &named_arguments,
                                                         ErrorData &error) {
	optional_idx best_function;
	idx_t lowest_cost = NumericLimits<idx_t>::Maximum();
	vector<idx_t> candidate_functions;
	for (idx_t f_idx = 0; f_idx < functions.functions.size(); f_idx++) {
		auto &func = functions.functions[f_idx];
		// check the arguments of the function
		auto bind_cost = BindFunctionCost(func, arguments, named_arguments);
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
		Identifier catalog_name;
		Identifier schema_name;
		for (auto &f : functions.functions) {
			if (catalog_name.empty() && !f.catalog_name.empty()) {
				catalog_name = f.catalog_name;
			}
			if (schema_name.empty() && !f.schema_name.empty()) {
				schema_name = f.schema_name;
			}
			candidates.push_back(f.ToString());
		}
		error = ErrorData(BinderException::NoMatchingFunction(catalog_name, schema_name, name, arguments,
		                                                      named_arguments, candidates));
		return candidate_functions;
	}
	candidate_functions.push_back(best_function.GetIndex());
	return candidate_functions;
}

template <class T>
static optional_idx
MultipleCandidateException(const Identifier &catalog_name, const Identifier &schema_name, const Identifier &name,
                           const FunctionSet<T> &functions, const vector<idx_t> &candidate_functions,
                           const vector<LogicalType> &arguments,
                           const vector<pair<Identifier, LogicalType>> &named_arguments, ErrorData &error) {
	D_ASSERT(functions.functions.size() > 1);
	// there are multiple possible function definitions
	// throw an exception explaining which overloads are there
	string call_str = Function::CallToString(catalog_name, schema_name, name, arguments, named_arguments);
	string candidate_str;
	for (auto &conf : candidate_functions) {
		const auto &f = functions.GetFunctionByOffset(conf);
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
optional_idx FunctionBinder::BindFunctionFromArguments(const Identifier &name, const FunctionSet<T> &functions,
                                                       const vector<LogicalType> &arguments,
                                                       const vector<pair<Identifier, LogicalType>> &named_arguments,
                                                       ErrorData &error) {
	auto candidate_functions = BindFunctionsFromArguments(name, functions, arguments, named_arguments, error);
	if (candidate_functions.empty()) {
		// No candidates, return an invalid index.
		return optional_idx();
	}
	if (candidate_functions.size() > 1) {
		// Multiple candidates, check if there are any unknown arguments.
		for (auto &arg_type : arguments) {
			if (arg_type.IsUnknown()) {
				// We cannot resolve the parameters to a function.
				throw ParameterNotResolvedException();
			}
		}
		auto catalog_name = functions.functions.size() > 0 ? functions.functions[0].GetCatalogName() : Identifier();
		auto schema_name = functions.functions.size() > 0 ? functions.functions[0].GetSchemaName() : Identifier();
		return MultipleCandidateException(catalog_name, schema_name, name, functions, candidate_functions, arguments,
		                                  named_arguments, error);
	}
	return candidate_functions[0];
}

template <class T>
static bool AnyOverloadSupportsImplicitArgumentNames(const FunctionSet<T> &functions) {
	for (auto &func : functions.functions) {
		if (func.GetProperties().GetCaptureArgumentAliases()) {
			return true;
		}
	}
	return false;
}

static optional_idx PositionalAfterNamedArgumentError(const vector<pair<Identifier, unique_ptr<Expression>>> &arguments,
                                                      ErrorData &error) {
	bool seen_named = false;
	for (const auto &[name, expr] : arguments) {
		if (!name.empty()) {
			seen_named = true;
			continue;
		}
		if (seen_named) {
			error = ErrorData(BinderException(
			    expr->GetQueryLocation(), "Positional argument '%s' cannot follow named arguments in function call.",
			    expr->ToString()));
			return optional_idx();
		}
	}
	throw InternalException("ThrowPositionalAfterNamed called without a positional-after-named argument");
}

template <class T>
optional_idx FunctionBinder::BindFunctionFromArguments(const Identifier &name, const FunctionSet<T> &functions,
                                                       vector<pair<Identifier, unique_ptr<Expression>>> &arguments,
                                                       ErrorData &error) {
	// First, attempt a regular bind, splitting the arguments into positional + named just once for all overloads.
	vector<LogicalType> positional;
	vector<pair<Identifier, LogicalType>> named;

	if (TrySplitArgumentTypes(arguments, positional, named)) {
		return BindFunctionFromArguments(name, functions, positional, named, error);
	}

	// The split failed because a positional argument follows a named one.
	// Check if there is any overload that supports implicit argument names.
	if (AnyOverloadSupportsImplicitArgumentNames(functions)) {
		// If so, we can attempt to salvage the call by implicitly naming the positional arguments and retrying again
		for (auto &[name, expr] : arguments) {
			if (name.empty()) {
				name = expr->GetAlias();
			}
		}

		positional.clear();
		named.clear();

		if (TrySplitArgumentTypes(arguments, positional, named)) {
			return BindFunctionFromArguments(name, functions, positional, named, error);
		}
	}

	// No overload could rescue the positional-after-named call, give a clear error.
	return PositionalAfterNamedArgumentError(arguments, error);
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const ScalarFunctionSet &functions,
                                          const vector<LogicalType> &regular_args,
                                          const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, regular_args, keyword_args, error);
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const AggregateFunctionSet &functions,
                                          const vector<LogicalType> &regular_args,
                                          const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, regular_args, keyword_args, error);
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const WindowFunctionSet &functions,
                                          const vector<LogicalType> &regular_args,
                                          const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, regular_args, keyword_args, error);
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const TableFunctionSet &functions,
                                          const vector<LogicalType> &regular_args,
                                          const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error) {
	return BindFunctionFromArguments(name, functions, regular_args, keyword_args, error);
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const PragmaFunctionSet &functions,
                                          vector<Value> &parameters, ErrorData &error) {
	vector<LogicalType> types;
	for (auto &value : parameters) {
		types.push_back(value.type());
	}
	auto entry = BindFunctionFromArguments(name, functions, types, {}, error);
	if (!entry.IsValid()) {
		error.Throw();
	}
	const auto &candidate_function = functions.GetFunctionByOffset(entry.GetIndex());
	// cast the input parameters
	for (idx_t i = 0; i < parameters.size(); i++) {
		auto target_type = i < candidate_function.GetArguments().size() ? candidate_function.GetArguments()[i]
		                                                                : candidate_function.GetVarArgs();
		parameters[i] = parameters[i].CastAs(context, target_type);
	}
	return entry;
}

pair<vector<LogicalType>, vector<pair<Identifier, LogicalType>>>
FunctionBinder::GetArgumentsFromExpressions(const vector<unique_ptr<Expression>> &regular_args,
                                            const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args) {
	vector<LogicalType> regular_arg_types;
	vector<pair<Identifier, LogicalType>> keyword_arg_types;

	for (auto &arg : regular_args) {
		regular_arg_types.push_back(ExpressionBinder::GetExpressionReturnType(*arg));
	}
	for (auto &kwarg : keyword_args) {
		keyword_arg_types.emplace_back(kwarg.first, ExpressionBinder::GetExpressionReturnType(*kwarg.second));
	}
	return {std::move(regular_arg_types), std::move(keyword_arg_types)};
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const ScalarFunctionSet &functions,
                                          const vector<unique_ptr<Expression>> &regular_args,
                                          const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
                                          ErrorData &error) {
	auto [args, kwargs] = GetArgumentsFromExpressions(regular_args, keyword_args);
	return BindFunctionFromArguments(name, functions, args, kwargs, error);
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const AggregateFunctionSet &functions,
                                          const vector<unique_ptr<Expression>> &regular_args,
                                          const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
                                          ErrorData &error) {
	auto [args, kwargs] = GetArgumentsFromExpressions(regular_args, keyword_args);
	return BindFunctionFromArguments(name, functions, args, kwargs, error);
}

optional_idx FunctionBinder::BindFunction(const Identifier &name, const TableFunctionSet &functions,
                                          const vector<unique_ptr<Expression>> &regular_args,
                                          const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
                                          ErrorData &error) {
	auto [args, kwargs] = GetArgumentsFromExpressions(regular_args, keyword_args);
	return BindFunctionFromArguments(name, functions, args, kwargs, error);
}

enum class LogicalTypeComparisonResult : uint8_t { IDENTICAL_TYPE, TARGET_IS_ANY, DIFFERENT_TYPES };

static LogicalTypeComparisonResult RequiresCast(const LogicalType &source_type, const LogicalType &target_type) {
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

static bool TypeRequiresPrepare(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return true;
	}
	if (type.id() == LogicalTypeId::LIST) {
		return TypeRequiresPrepare(ListType::GetChildType(type));
	}
	return false;
}

static LogicalType PrepareTypeForCastRecursive(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return AnyType::GetTargetType(type);
	}
	if (type.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(PrepareTypeForCastRecursive(ListType::GetChildType(type)));
	}
	return type;
}

static void PrepareTypeForCast(LogicalType &type) {
	if (!TypeRequiresPrepare(type)) {
		return;
	}
	type = PrepareTypeForCastRecursive(type);
}

void FunctionBinder::CastToFunctionArguments(BoundSimpleFunction &function, vector<unique_ptr<Expression>> &children) {
	for (auto &arg : function.GetArguments()) {
		PrepareTypeForCast(arg);
	}

	// Varargs should be expanded by this point
	// If not, the function has somehow added more argument expressions during binding, which is not allowed.
	// There is one exception, count(*) which can be invoked internally with fewer arguments than the function
	// definition. That should be fixed separately though.
	D_ASSERT(children.size() <= function.GetArguments().size());

	for (idx_t i = 0; i < children.size(); i++) {
		auto &target_type = function.GetArguments()[i];
		if (target_type.id() == LogicalTypeId::STRING_LITERAL || target_type.id() == LogicalTypeId::INTEGER_LITERAL) {
			throw InternalException(
			    "Function %s returned a STRING_LITERAL or INTEGER_LITERAL type - return an explicit type instead",
			    function.GetName());
		}
		target_type.Verify();
		// don't cast lambda children, they get removed before execution
		if (children[i]->GetReturnType().id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		// check if the type of child matches the type of function argument
		// if not we need to add a cast
		auto cast_result = RequiresCast(children[i]->GetReturnType(), target_type);
		// except for one special case: if the function accepts ANY argument
		// in that case we don't add a cast
		if (cast_result == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			children[i] = BoundCastExpression::AddCastToType(context, std::move(children[i]), target_type);
		}
	}
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const Identifier &schema, const Identifier &name,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	// bind the function
	auto &function = Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(context, schema, name);
	D_ASSERT(function.type == CatalogType::SCALAR_FUNCTION_ENTRY);
	return BindScalarFunction(function, std::move(children), error, is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunctionCatalogEntry &func,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	arguments.reserve(children.size());
	for (auto &child : children) {
		arguments.emplace_back(string(), std::move(child));
	}
	return BindScalarFunction(func, std::move(arguments), error, is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunctionCatalogEntry &func,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> arguments,
                                                          ErrorData &error, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	// select the best matching overload (this may name positional arguments by their alias for functions that opt
	// into implicit argument naming, e.g. struct_pack/row)
	auto best_function = BindFunctionFromArguments(func.name, func.functions, arguments, error);
	if (!best_function.IsValid()) {
		return nullptr;
	}

	// found a matching function!
	const auto &bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());

	// now that the overload is fixed, split the arguments into their final positional/named children
	auto [regular_args, keyword_args] = SplitArguments(std::move(arguments));

	// If any of the parameters are NULL, the function will just be replaced with a NULL constant.
	// We try to give the NULL constant the correct type, but we have to do this without binding the function,
	// because functions with DEFAULT_NULL_HANDLING should not have to deal with NULL inputs in their bind code.
	// Some functions may have an invalid default return type, as they must be bound to infer the return type.
	// In those cases, we default to SQLNULL.
	const auto return_type_if_null =
	    bound_function.GetReturnType().IsComplete() ? bound_function.GetReturnType() : LogicalType::SQLNULL;
	if (bound_function.GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		for (auto &child : regular_args) {
			if (child->GetReturnType() == LogicalTypeId::SQLNULL) {
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
		for (auto &child : keyword_args) {
			if (child.second->GetReturnType() == LogicalTypeId::SQLNULL) {
				return make_uniq<BoundConstantExpression>(Value(return_type_if_null));
			}
			if (!child.second->IsFoldable()) {
				continue;
			}
			Value result;
			if (!ExpressionExecutor::TryEvaluateScalar(context, *child.second, result)) {
				continue;
			}
			if (result.IsNull()) {
				return make_uniq<BoundConstantExpression>(Value(return_type_if_null));
			}
		}
	}
	return BindScalarFunction(bound_function, std::move(regular_args), std::move(keyword_args), is_operator, binder);
}

static bool RequiresCollationPropagation(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR && !type.HasAlias();
}

static string ExtractCollation(const vector<unique_ptr<Expression>> &children) {
	string collation;
	for (auto &arg : children) {
		if (!RequiresCollationPropagation(arg->GetReturnType())) {
			// not a varchar column
			continue;
		}
		auto child_collation = StringType::GetCollation(arg->GetReturnType());
		if (collation.empty()) {
			collation = child_collation;
		} else if (!child_collation.empty() && collation != child_collation) {
			throw BinderException("Cannot combine types with different collation!");
		}
	}
	return collation;
}

static void PropagateCollations(ClientContext &, BoundSimpleFunction &bound_function,
                                vector<unique_ptr<Expression>> &children) {
	if (!RequiresCollationPropagation(bound_function.GetReturnType())) {
		// we only need to propagate if the function returns a varchar
		return;
	}
	auto collation = ExtractCollation(children);
	if (collation.empty()) {
		// no collation to propagate
		return;
	}
	// propagate the collation to the return type
	auto collation_type = LogicalType::VARCHAR_COLLATION(std::move(collation));
	bound_function.SetReturnType(std::move(collation_type));
}

static void PushCollations(ClientContext &context, BoundSimpleFunction &bound_function,
                           vector<unique_ptr<Expression>> &children, CollationType type) {
	auto collation = ExtractCollation(children);
	if (collation.empty()) {
		// no collation to push
		return;
	}
	// push collation into the return type if required
	auto collation_type = LogicalType::VARCHAR_COLLATION(std::move(collation));
	if (RequiresCollationPropagation(bound_function.GetReturnType())) {
		bound_function.SetReturnType(collation_type);
	}
	// push collations to the children
	for (auto &arg : children) {
		if (RequiresCollationPropagation(arg->GetReturnType())) {
			// if this is a varchar type - propagate the collation
			arg->SetReturnType(collation_type);
		}
		// now push the actual collation handling
		ExpressionBinder::PushCollation(context, arg, arg->GetReturnType(), type);
	}
}

static void HandleCollations(ClientContext &context, BoundSimpleFunction &bound_function,
                             const FunctionProperties &props, vector<unique_ptr<Expression>> &children) {
	switch (props.GetCollationHandling()) {
	case FunctionCollationHandling::IGNORE_COLLATIONS:
		// explicitly ignoring collation handling
		break;
	case FunctionCollationHandling::PROPAGATE_COLLATIONS:
		PropagateCollations(context, bound_function, children);
		break;
	case FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS:
		// first propagate, then push collations to the children
		PushCollations(context, bound_function, children, CollationType::COMBINABLE_COLLATIONS);
		break;
	default:
		throw InternalException("Unrecognized collation handling");
	}
}

static void InferTemplateType(ClientContext &context, const LogicalType &source, const LogicalType &target,
                              case_insensitive_map_t<vector<LogicalType>> &bindings, const Expression &current_expr,
                              const BoundSimpleFunction &function) {
	if (target.id() == LogicalTypeId::UNKNOWN || target.id() == LogicalTypeId::SQLNULL) {
		// If the actual type is unknown, we cannot infer anything more.
		// Therefore, we map all remaining templates in the source to UNKNOWN or SQLNULL, if not already inferred to
		// something else

		// This might seem a bit strange, why not just not set the binding and error out later when we try to substitute
		// all templates? Well, this is how bindings for most nested functions already work, they simply propagate the
		// UNKNOWN/SQLNULL. The binder will later check for UNKNOWN/SQLNULL in return types and if it finds one, insert
		// a dummy cast to INT32 so that the function can be executed without errors (and just return NULLs).

		TypeVisitor::Contains(source, [&](const LogicalType &child) {
			if (child.id() == LogicalTypeId::TEMPLATE) {
				const auto index = TemplateType::GetName(child);
				if (bindings.find(index) == bindings.end()) {
					// not found, add the binding
					bindings[index] = {target.id()};
				}
			}
			return false; // continue visiting
		});
		return;
	}

	// If the source is a template type, we bind it, or try to unify its existing binding with the target type.
	if (source.id() == LogicalTypeId::TEMPLATE) {
		const auto &index = TemplateType::GetName(source);
		auto it = bindings.find(index);
		if (it == bindings.end()) {
			// not found, add the binding
			bindings[index] = {target};
			return;
		}
		if (it->second.back() == target) {
			// already bound to the same type
			return;
		}

		// Try to unify (promote) the type candidates
		LogicalType result;
		if (LogicalType::TryGetMaxLogicalType(context, it->second.back(), target, result)) {
			// Type unification was successful
			if (it->second.back() != result) {
				// update the binding
				it->second.push_back(target);
				it->second.push_back(std::move(result)); // Push the new promoted type
			}
			return;
		}

		// If we reach here, it means the types are incompatible
		string msg =
		    StringUtil::Format("Cannot deduce template type '%s' in function: '%s'\nType '%s' was inferred to be:\n",
		                       TemplateType::GetName(source), function.ToString(), TemplateType::GetName(source));
		const auto &steps = it->second;

		for (idx_t i = 0; i < steps.size(); i += 2) {
			if (i == 0) {
				// Normalize the first step to ensure it is a valid type
				msg += StringUtil::Format(" - '%s', from first occurrence\n", steps[i].ToString());
			} else {
				msg += StringUtil::Format(" - '%s', by promoting '%s' + '%s'\n", steps[i].ToString(),
				                          steps[i - 2].ToString(), steps[i - 1]);
			}
		}
		msg += StringUtil::Format(" - '%s', which is incompatible with previously inferred type!", target.ToString());
		throw BinderException(current_expr.GetQueryLocation(), msg);
	}

	// Otherwise, recurse downwards into nested types, and try to infer nested type members
	// This only works if the source and target types are completely defined (excluding templates),
	// i.e. they have aux info.
	if (!(source.IsNested() && target.IsNested() && source.AuxInfo() && target.AuxInfo())) {
		return;
	}

	switch (source.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		if ((source.id() == LogicalTypeId::ARRAY || source.id() == LogicalTypeId::LIST) &&
		    (target.id() == LogicalTypeId::LIST || target.id() == LogicalTypeId::ARRAY)) {
			const auto &source_child =
			    source.id() == LogicalTypeId::LIST ? ListType::GetChildType(source) : ArrayType::GetChildType(source);
			const auto &target_child =
			    target.id() == LogicalTypeId::LIST ? ListType::GetChildType(target) : ArrayType::GetChildType(target);
			InferTemplateType(context, source_child, target_child, bindings, current_expr, function);
		}
	} break;
	case LogicalTypeId::MAP: {
		// Map is only implicitly castable to map, so we only need to handle this case here/
		if (target.id() == LogicalTypeId::MAP) {
			const auto &source_key = MapType::KeyType(source);
			const auto &source_val = MapType::ValueType(source);
			const auto &target_key = MapType::KeyType(target);
			const auto &target_val = MapType::ValueType(target);

			InferTemplateType(context, source_key, target_key, bindings, current_expr, function);
			InferTemplateType(context, source_val, target_val, bindings, current_expr, function);
		}
	} break;
	case LogicalTypeId::UNION: {
		// TODO: Support union types with template member types.
		throw NotImplementedException("Union types cannot infer templated member types yet!");
	} break;
	case LogicalTypeId::STRUCT: {
		// Structs are only implicitly castable to structs, so we only need to handle this case here.
		if (target.id() == LogicalTypeId::STRUCT && StructType::IsUnnamed(source)) {
			const auto &source_children = StructType::GetChildTypes(source);
			const auto &target_children = StructType::GetChildTypes(target);

			const auto common_children = MinValue(source_children.size(), target_children.size());
			for (idx_t i = 0; i < common_children; i++) {
				const auto &source_child_type = source_children[i].second;
				const auto &target_child_type = target_children[i].second;
				InferTemplateType(context, source_child_type, target_child_type, bindings, current_expr, function);
			}
		} else {
			// TODO: Support named structs with template child types.
			throw NotImplementedException("Named structs cannot infer templated child types yet!");
		}
	} break;
	default:
		break; // no template type to infer
	}
}

static void SubstituteTemplateType(LogicalType &type, case_insensitive_map_t<vector<LogicalType>> &bindings,
                                   const Identifier &function_name) {
	// Replace all template types in with their bound concrete types.
	type = TypeVisitor::VisitReplace(type, [&](const LogicalType &t) -> LogicalType {
		if (t.id() == LogicalTypeId::TEMPLATE) {
			const auto index = TemplateType::GetName(t);
			auto it = bindings.find(index);
			if (it != bindings.end()) {
				// found a binding, return the concrete type
				return LogicalType::NormalizeType(it->second.back());
			}

			// If we reach here, the template type was not bound to any concrete type.
			// We dont throw an error here, but give users a chance to handle unresolved template type later in the
			// "bind_scalar_function_t" callback. We then throw an error if the template type is still not bound
			// in the "CheckTemplateTypesResolved" method afterwards.
		}
		return t;
	});
}

void FunctionBinder::ResolveTemplateTypes(BoundSimpleFunction &bound_function,
                                          const vector<unique_ptr<Expression>> &children) {
	case_insensitive_map_t<vector<LogicalType>> bindings;
	vector<reference<LogicalType>> to_substitute;

	// First, we need to infer the template types from the children.
	for (idx_t i = 0; i < bound_function.GetArguments().size(); i++) {
		auto &param = bound_function.GetArguments()[i];

		// If the parameter is not templated, we can skip it.
		if (param.IsTemplated()) {
			auto actual = ExpressionBinder::GetExpressionReturnType(*children[i]);
			InferTemplateType(context, param, actual, bindings, *children[i], bound_function);

			to_substitute.emplace_back(param);
		}
	}

	// If the return type is templated, we need to substitute it as well
	if (bound_function.GetReturnType().IsTemplated()) {
		to_substitute.emplace_back(bound_function.GetReturnType());
	}

	// Finally, substitute all template types in the bound function with their concrete types.
	for (auto &templated_type : to_substitute) {
		SubstituteTemplateType(templated_type, bindings, bound_function.GetName());
	}
}

static void VerifyTemplateType(const LogicalType &type, const Identifier &function_name) {
	TypeVisitor::Contains(type, [&](const LogicalType &type) {
		if (type.id() == LogicalTypeId::TEMPLATE) {
			const auto msg =
			    "Function '%s' has a template parameter type '%s' that could not be resolved to a concrete type";
			throw BinderException(msg, function_name, TemplateType::GetName(type));
		}
		return false; // continue visiting
	});
}

// Verify that all template types are bound to concrete types.
void FunctionBinder::CheckTemplateTypesResolved(const BoundSimpleFunction &bound_function) {
	for (const auto &arg : bound_function.GetArguments()) {
		VerifyTemplateType(arg, bound_function.GetName());
	}
	VerifyTemplateType(bound_function.GetReturnType(), bound_function.GetName());
}

// Drain all named argument and insert them in the correct position according to the function signature.
// Also insert default arguments where needed.
static void ResolveArguments(const SimpleFunction &function, vector<unique_ptr<Expression>> &arguments,
                             vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	const auto &sig = function.GetSignature();

	const auto kwargs_offset = arguments.size();

	// Reserve space for the named arguments
	if (arguments.size() < sig.GetParameterCount()) {
		arguments.resize(sig.GetParameterCount());
	}

	identifier_set_t seen_names;

	vector<unique_ptr<Expression>> trailing_kwargs;

	// We now need to reorder them to match the function signature, before appending them to the argument list.
	for (idx_t kwarg_idx = 0; kwarg_idx < named_arguments.size(); kwarg_idx++) {
		auto &[name, arg] = named_arguments[kwarg_idx];
		const auto location = arg->GetQueryLocation();

		if (name.empty()) {
			// Somehow this was not a named argument, throw an error.
			throw BinderException(location,
			                      "Positional arguments cannot follow named arguments in a function call to '%s'",
			                      function.GetName());
		}

		if (seen_names.count(name)) {
			// This should also not really happen when invoked through SQL
			throw BinderException(location, "Duplicate named argument '%s' in function call to '%s'",
			                      name.GetIdentifierName(), function.GetName());
		}

		seen_names.insert(name);

		const auto opt_param_idx = sig.GetParameterIndexByName(name);
		if (!opt_param_idx.IsValid()) {
			if (!sig.HasVarArgs()) {
				throw BinderException(location, "Function '%s' does not have a parameter named '%s'",
				                      function.GetName(), name);
			}

			// This is a named vararg argument, come back for it later
			trailing_kwargs.push_back(std::move(arg));
			continue;
		}

		const auto param_idx = opt_param_idx.GetIndex();

		if (param_idx < kwargs_offset) {
			throw BinderException(location,
			                      "Named argument '%s' cannot be used for parameter '%s' because it has already "
			                      "been provided as a positional argument in function call to '%s'",
			                      arg->ToString(), name, function.GetName());
		}

		// Move it into the correct reserved position
		arguments[param_idx] = std::move(arg);
	}

	// Fill out missing arguments with default values if they exist, otherwise throw an error.
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		if (arguments[i]) {
			continue;
		}

		const auto &param = sig.GetParameter(i);

		if (param.HasDefaultValue()) {
			arguments[i] = make_uniq<BoundConstantExpression>(*param.GetDefaultValue());
			arguments[i]->SetAlias(param.GetName());

		} else {
			throw BinderException("Missing value for parameter '%s' in function call to '%s'", param.GetName(),
			                      function.GetName());
		}
	}

	// Now spread out any trailing named vararg arguments into the remaining argument slots, wherever they may be
	idx_t kwarg_idx = 0;
	for (idx_t slot_idx = kwargs_offset; slot_idx < arguments.size(); slot_idx++) {
		if (!arguments[slot_idx]) {
			arguments[slot_idx] = std::move(trailing_kwargs[kwarg_idx++]);
		}
	}

	// And if there are still some left, just append them to the end
	idx_t kwargs_remaining = trailing_kwargs.size() - kwarg_idx;
	while (kwargs_remaining) {
		arguments.push_back(std::move(trailing_kwargs[kwarg_idx++]));
		kwargs_remaining--;
	}
}

pair<BoundScalarFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveFunction(const ScalarFunction &function, vector<unique_ptr<Expression>> &arguments,
                                vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	// Reorder named args
	ResolveArguments(function, arguments, named_arguments);

	// Make a BoundScalarFunction out of the ScalarFunction, so we can store bind info and other properties in it.
	BoundScalarFunction bound_function(function);

	// Expand varargs if necessary
	if (function.HasVarArgs()) {
		const auto &varargs_type = function.GetVarArgs();
		for (idx_t i = function.GetSignature().GetParameterCount(); i < arguments.size(); i++) {
			bound_function.GetArguments().push_back(varargs_type);
		}
	}

	// Attempt to resolve template types, before we call the "Bind" callback.
	ResolveTemplateTypes(bound_function, arguments);

	unique_ptr<FunctionData> bind_info;

	if (bound_function.HasBindCallback()) {
		BindScalarFunctionInput input(context, bound_function, arguments, binder);
		bind_info = bound_function.GetBindCallback()(input);
	}

	// After the "bind" callback, we verify that all template types are bound to concrete types.
	CheckTemplateTypesResolved(bound_function);

	if (bound_function.HasModifiedDatabasesCallback() && binder) {
		auto &properties = binder->GetStatementProperties();
		FunctionModifiedDatabasesInput input(bind_info, properties);
		bound_function.GetModifiedDatabasesCallback()(context, input);
	}

	HandleCollations(context, bound_function, bound_function.GetProperties(), arguments);

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, arguments);

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunction &function,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder) {
	return BindScalarFunction(function, std::move(children), {}, is_operator, binder);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunction &function,
                                                          vector<unique_ptr<Expression>> children,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                                          bool is_operator, optional_ptr<Binder> binder) {
	auto [bound_function, bind_info] = ResolveFunction(function, children, keyword_args);

	unique_ptr<Expression> result;

	auto result_func = make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(children),
	                                                      std::move(bind_info), is_operator);

	if (result_func->Function().HasBindExpressionCallback()) {
		// if a bind_expression callback is registered - call it and emit the resulting expression
		FunctionBindExpressionInput input(context, result_func->FunctionMutable(), result_func->BindInfoMutable().get(),
		                                  result_func->GetChildrenMutable());
		result = result_func->Function().GetBindExpressionCallback()(input);
	}

	if (!result) {
		result = std::move(result_func);
	}

	return result;
}

pair<BoundAggregateFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> &children,
                                vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	// Reorder named args
	ResolveArguments(function, children, named_arguments);

	// Make a BoundFunction out of the func
	BoundAggregateFunction bound_function(function);

	// Expand varargs if necessary
	if (function.HasVarArgs()) {
		const auto &varargs_type = function.GetVarArgs();
		for (idx_t i = function.GetSignature().GetParameterCount(); i < children.size(); i++) {
			bound_function.GetArguments().push_back(varargs_type);
		}
	}

	ResolveTemplateTypes(bound_function, children);

	unique_ptr<FunctionData> bind_info;

	if (bound_function.GetCallbacks().HasBindCallback()) {
		BindAggregateFunctionInput input(context, bound_function, children);
		bind_info = bound_function.GetCallbacks().GetBindCallback()(input);

		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
	}

	CheckTemplateTypesResolved(bound_function);

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children);

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(const AggregateFunction &function,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	return BindAggregateFunction(function, std::move(children), {}, std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	auto [bound_function, bind_info] = ResolveFunction(function, children, keyword_args);

	return make_uniq<BoundAggregateExpression>(std::move(bound_function), std::move(children), std::move(filter),
	                                           std::move(bind_info), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const AggregateFunctionCatalogEntry &func,
                                      vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	// select the best matching overload (this may name positional arguments by their alias for functions that opt
	// into implicit argument naming, e.g. struct_pack/row)
	auto best_function = BindFunctionFromArguments(func.name, func.functions, arguments, error);
	if (!best_function.IsValid()) {
		return nullptr;
	}

	// found a matching function!
	const auto &bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());

	// now that the overload is fixed, split the arguments into their final positional/named children
	auto [regular_args, keyword_args] = SplitArguments(std::move(arguments));

	return BindAggregateFunction(bound_function, std::move(regular_args), std::move(keyword_args), std::move(filter),
	                             aggr_type);
}

pair<BoundWindowFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveFunction(const WindowFunction &function, vector<unique_ptr<Expression>> &children,
                                vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
                                optional_ptr<vector<OrderByNode>> orders,
                                optional_ptr<vector<OrderByNode>> arg_orders) {
	// Reorder named args
	ResolveArguments(function, children, named_arguments);

	BoundWindowFunction bound_function(function);

	// Expand varargs if necessary
	if (function.HasVarArgs()) {
		const auto &varargs_type = function.GetVarArgs();
		for (idx_t i = function.GetSignature().GetParameterCount(); i < children.size(); i++) {
			bound_function.GetArguments().push_back(varargs_type);
		}
	}

	ResolveTemplateTypes(bound_function, children);

	unique_ptr<FunctionData> bind_info;

	if (bound_function.HasBindCallback()) {
		BindWindowFunctionInput input(context, bound_function, children, orders, arg_orders);
		bind_info = bound_function.GetBindCallback()(input);
		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
	}

	CheckTemplateTypesResolved(bound_function);

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function, children);

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(const WindowFunction &function, vector<unique_ptr<Expression>> children,
                                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                   vector<OrderByNode> &orders, vector<OrderByNode> &arg_orders) {
	auto [bound_function, bind_info] = ResolveFunction(function, children, keyword_args, orders, arg_orders);
	auto return_type = bound_function.GetReturnType();

	auto window = make_uniq<BoundWindowFunction>(std::move(bound_function));
	auto result = make_uniq<BoundWindowExpression>(return_type, nullptr, std::move(window), std::move(bind_info));
	result->GetChildrenMutable() = std::move(children);

	return result;
}

unique_ptr<BoundWindowExpression> FunctionBinder::BindWindowFunction(const WindowFunction &function,
                                                                     vector<unique_ptr<Expression>> children,
                                                                     vector<OrderByNode> &orders,
                                                                     vector<OrderByNode> &arg_orders) {
	vector<pair<Identifier, unique_ptr<Expression>>> empty_keyword_args;
	return BindWindowFunction(function, std::move(children), std::move(empty_keyword_args), orders, arg_orders);
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(const WindowFunctionCatalogEntry &func,
                                   vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
                                   vector<OrderByNode> &orders, vector<OrderByNode> &arg_orders) {
	// select the best matching overload
	auto best_function = BindFunctionFromArguments(func.name, func.functions, arguments, error);
	if (!best_function.IsValid()) {
		return nullptr;
	}

	// found a matching function!
	const auto &bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());

	// now that the overload is fixed, split the arguments into their final positional/named children
	auto [regular_args, keyword_args] = SplitArguments(std::move(arguments));

	return BindWindowFunction(bound_function, std::move(regular_args), std::move(keyword_args), orders, arg_orders);
}

} // namespace duckdb
